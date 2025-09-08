use crate::configuration::Configuration;
use crate::cputime::{self, ProcPidStats};
use crate::siegeurls::{BodyData, UrlEntry};
use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use crossbeam::select;
use log::{debug, log_enabled, Level};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{StatusCode, Url};
use std::cmp::min;
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::scope;
use std::time::{Duration, Instant};

struct CallResult {
    status: StatusCode,
    bytes_sent: u64,
    bytes_received: u64,
}

struct TotalsSet {
    success: Totals,
    client_fail: Totals,
    server_fail: Totals,
    conn_error: Totals,
}

struct Totals {
    count: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
    elapsed: AtomicU64,
}

struct LocalTotals {
    count: u64,
    bytes_up: u64,
    bytes_down: u64,
    elapsed: u64,
    error_count: u64,
    cpu: ProcPidStats,
}

impl Totals {
    fn into_local(&self) -> LocalTotals {
        LocalTotals {
            count: self.count.load(Ordering::Relaxed),
            bytes_up: self.bytes_up.load(Ordering::Relaxed),
            bytes_down: self.bytes_down.load(Ordering::Relaxed),
            elapsed: self.elapsed.load(Ordering::Relaxed),
            error_count: 0,
            cpu: ProcPidStats {
                elapsed: Duration::ZERO,
                user: Duration::ZERO,
                system: Duration::ZERO,
            },
        }
    }
}

impl TotalsSet {
    fn into_local(&self) -> (LocalTotals, LocalTotals, LocalTotals, LocalTotals) {
        (
            self.success.into_local(),
            self.client_fail.into_local(),
            self.server_fail.into_local(),
            self.conn_error.into_local(),
        )
    }
}

// useful jmeter values:
// "timeStamp","elapsed","label","responseCode","threadName","success","bytes","allThreads"
#[derive(Debug)]
struct Sample {
    timestamp: DateTime<Utc>,
    elapsed: Duration,
    success: bool,
    response_code: String,
    bytes_up: u64,
    bytes_down: u64,
    method: String,
    url: String,
    thread: usize,
    threads: usize,
}

impl fmt::Display for Sample {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{},{}",
            self.timestamp.timestamp_millis(),
            self.elapsed.as_millis(),
            self.success,
            self.response_code,
            self.bytes_up,
            self.bytes_down,
            self.method,
            self.url,
            self.thread,
            self.threads
        )
    }
}

fn hit_target(
    client: &Client,
    baseurl: &Url,
    base_headers: &Vec<String>,
    url_entry: &UrlEntry,
) -> Result<CallResult, Box<dyn std::error::Error>> {
    let url = baseurl.join(&url_entry.urlpart.clone()).unwrap();
    let method = url_entry.method.clone();
    let req_builder = client.request(method, url);

    let mut req_builder = if let Some(content_type) = &url_entry.content_type {
        req_builder.header("Content-Type", content_type)
    } else {
        req_builder
    };

    let mut headers = HeaderMap::with_capacity(base_headers.len());
    for header in base_headers.iter() {
        if let Some((name, value)) = header.split_once(':') {
            let name = HeaderName::from_bytes(name.trim().as_bytes())?;
            let value = HeaderValue::from_bytes(value.trim().as_bytes())?;
            headers.append(name, value);
        }
    }
    req_builder = req_builder.headers(headers);

    let (bytes_sent, req) = match &url_entry.body {
        BodyData::Content(body) => (body.len() as u64, req_builder.body(body.clone()).send()?),
        BodyData::File(path) => {
            let file = std::fs::File::open(path)?;
            (file.metadata()?.len(), req_builder.body(file).send()?)
        }
        BodyData::None => (0u64, req_builder.send()?),
    };
    let status = req.status();
    let bytes_received = req.text()?.len() as u64;

    Ok(CallResult {
        status,
        bytes_sent,
        bytes_received,
    })
}

fn traffic_user(
    baseurl: Url,
    base_headers: Vec<String>,
    thread: usize,
    threads: usize,
    rx: Receiver<Option<UrlEntry>>,
    client: Client,
    result_tx: Sender<Sample>,
) -> usize {
    let mut count = 0;
    let mut succeess_count = 0;
    let mut total_bytes = 0;
    let mut total_duration = Duration::ZERO;
    let mut conn_error_count = 0;
    let mut conn_error_duration = Duration::ZERO;

    loop {
        let url_entry = rx.recv().unwrap();
        if url_entry.is_none() {
            // Got the signal to quit
            // (Ideally, this'd be a watch or broadcaster and we'd select
            // between that receiver and the "main" receiver, but this'll do)
            break;
        }
        let url_entry = url_entry.unwrap();
        let timestamp = Utc::now();
        let start = Instant::now();
        let entry = match hit_target(&client, &baseurl, &base_headers, &url_entry) {
            Result::Ok(v) => {
                let elapsed = start.elapsed();
                total_duration += elapsed;
                count += 1;
                total_bytes += v.bytes_received;
                if v.status.is_success() {
                    succeess_count += 1;
                }

                Sample {
                    timestamp,
                    elapsed,
                    success: v.status.is_success(),
                    response_code: v.status.as_str().to_string(),
                    bytes_up: v.bytes_sent,
                    bytes_down: v.bytes_received,
                    method: url_entry.method.to_string(),
                    url: url_entry.urlpart.to_string(),
                    thread,
                    threads,
                }
            }
            Result::Err(_) => {
                conn_error_count += 1;
                conn_error_duration += start.elapsed();
                let elapsed = start.elapsed();

                Sample {
                    timestamp,
                    elapsed,
                    success: false,
                    response_code: "conn_error".to_string(),
                    bytes_up: 0,
                    bytes_down: 0,
                    method: url_entry.method.to_string(),
                    url: url_entry.urlpart.to_string(),
                    thread,
                    threads,
                }
            }
        };
        if let Err(e) = result_tx.send(entry) {
            eprintln!("ERROR: Could not post result: {:?}", e);
        }
    }
    let error_notes = if conn_error_count == 0 {
        "".to_string()
    } else {
        format!(
            " with conn_errors={} total_error_duration_millis={}",
            conn_error_count,
            conn_error_duration.as_millis()
        )
    };
    debug!(
        "Thread made calls={} (success_total={}) total_bytes={} total_duration_millis={}{}",
        count,
        succeess_count,
        total_bytes,
        total_duration.as_millis(),
        error_notes
    );
    count
}

fn logger(rx: Receiver<Sample>, counters: Arc<TotalsSet>) {
    let mut log = BufWriter::new(File::create("results.log").unwrap());
    let mut count = 0;
    loop {
        let entry = rx.recv().unwrap();
        if entry.elapsed == Duration::MAX {
            // Got the signal to quit
            // (Ideally, this'd be a watch or broadcaster and we'd select
            // between that receiver and the "main" receiver, but this'll do)
            break;
        }
        let totals = if entry.response_code.starts_with("4") {
            &counters.client_fail
        } else if entry.response_code.starts_with("5") {
            &counters.server_fail
        } else if entry.response_code == "conn_error" {
            &counters.conn_error
        } else {
            &counters.success
        };
        totals.count.fetch_add(1, Ordering::Relaxed);
        totals
            .elapsed
            .fetch_add(entry.elapsed.as_millis() as u64, Ordering::Relaxed);
        totals.bytes_up.fetch_add(entry.bytes_up, Ordering::Relaxed);
        totals
            .bytes_down
            .fetch_add(entry.bytes_down, Ordering::Relaxed);

        count += 1;
        writeln!(log, "{}", entry).unwrap();
    }
    log.flush().unwrap();
    debug!("Logging complete. Received {} entries.", count);
}

fn report_stats(
    report_dt: DateTime<Utc>,
    job_elapsed: Duration,
    iter_elapsed: Duration,
    iter_req_ms_total: u64,
    iter_reqs: u64,
    iter_errs: u64,
    iter_bytes_up: u64,
    iter_bytes_down: u64,
    iter_cores: f32,
) {
    // Example:
    // 2025-09-08T06:18:01Z 0:00:01 1707req/s 1225ms/req 99.0%err 123KB/s:up 123KB/s:dn 21.0cores

    // Datetime and elapsed job time
    let (h, m, s) = cputime::duration_hms(job_elapsed);
    print!(
        "{} {}:{:02}:{:02}",
        report_dt.to_rfc3339_opts(SecondsFormat::Secs, true),
        h,
        m,
        s
    );

    let iter_ms = iter_elapsed.as_millis() as u64;

    // requests per time period
    let reqs_sec = iter_reqs * 1000 / iter_ms;
    if reqs_sec >= 10_000 {
        // " 10kreq/s"
        print!(" {:3.0}kreq/s", reqs_sec / 1000);
    } else {
        // 9999req/s
        print!(" {:4.0}req/s", reqs_sec);
    }

    // average response time
    let ms_req = if iter_reqs > 0 {
        iter_req_ms_total as f64 / iter_reqs as f64
    } else {
        0f64
    };
    if ms_req >= 1_000_000f64 {
        // " 1000s/req" (yikes if this happens)
        print!(" {:5.0}s/req", ms_req / 1000f64);
    } else if ms_req >= 100_000f64 {
        // 999.9s/req (yikes if this happens)
        print!(" {:5.1}s/req", ms_req / 1000f64);
    } else if ms_req >= 10_000f64 {
        // 99.99s/req
        print!(" {:5.2}s/req", ms_req / 1000f64);
    } else if ms_req < 0.001f64 && ms_req > 0f64 {
        print!(" {:4.0}ns/req", ms_req * 1_000_000f64);
    } else if ms_req < 1f64 && ms_req > 0f64 {
        print!(" {:4.0}μs/req", ms_req * 1000f64);
    } else {
        // 9999ms/req
        print!(" {:4.0}ms/req", ms_req);
    }

    // percentage of calls in error
    let err_perc = if iter_reqs > 0 {
        iter_errs as f64 / iter_reqs as f64 * 100f64
    } else {
        0f64
    };
    if err_perc >= 100f64 {
        // " 100%err"
        print!(" {:4.0}%err", err_perc);
    } else if err_perc >= 9.995f64 {
        // 99.9%err
        print!(" {:4.1}%err", err_perc);
    } else {
        // 9.99%err
        print!(" {:4.2}%err", err_perc);
    }

    // upload rate
    let rate_up_bytes = iter_bytes_up as f64 * 1000f64 / iter_ms as f64;
    if rate_up_bytes >= 100_000_000_000f64 {
        // " 100GB/s:up"
        print!(" {:4.0}GB/s:up", rate_up_bytes / 1_000_000_000f64);
    } else if rate_up_bytes >= 10_000_000_000f64 {
        // 99.9GB/s:up
        print!(" {:4.1}GB/s:up", rate_up_bytes / 1_000_000_000f64);
    } else if rate_up_bytes >= 1_000_000_000f64 {
        // 9.99GB/s:up
        print!(" {:4.2}GB/s:up", rate_up_bytes / 1_000_000_000f64);
    } else if rate_up_bytes >= 100_000_000f64 {
        // " 100MB/s:up"
        print!(" {:4.0}MB/s:up", rate_up_bytes / 1_000_000f64);
    } else if rate_up_bytes >= 10_000_000f64 {
        // 99.9MB/s:up
        print!(" {:4.1}MB/s:up", rate_up_bytes / 1_000_000f64);
    } else if rate_up_bytes >= 1_000_000f64 {
        // 9.99MB/s:up
        print!(" {:4.2}MB/s:up", rate_up_bytes / 1_000_000f64);
    } else if rate_up_bytes >= 100_000f64 {
        // " 100KB/s:up"
        print!(" {:4.0}KB/s:up", rate_up_bytes / 1_000f64);
    } else if rate_up_bytes >= 10_000f64 {
        // 99.9KB/s:up
        print!(" {:4.1}KB/s:up", rate_up_bytes / 1_000f64);
    } else {
        // 99999B/s:up
        print!(" {:5.0}B/s:up", rate_up_bytes);
    }

    // download rate
    let rate_down_bytes = iter_bytes_down as f64 * 1000f64 / iter_ms as f64;
    if rate_down_bytes >= 100_000_000_000f64 {
        // " 100GB/s:dn"
        print!(" {:4.0}GB/s:dn", rate_down_bytes / 1_000_000_000f64);
    } else if rate_down_bytes >= 10_000_000_000f64 {
        // 99.9GB/s:dn
        print!(" {:4.1}GB/s:dn", rate_down_bytes / 1_000_000_000f64);
    } else if rate_down_bytes >= 1_000_000_000f64 {
        // 9.99GB/s:dn
        print!(" {:4.2}GB/s:dn", rate_down_bytes / 1_000_000_000f64);
    } else if rate_down_bytes >= 100_000_000f64 {
        // " 100MB/s:dn"
        print!(" {:4.0}MB/s:dn", rate_down_bytes / 1_000_000f64);
    } else if rate_down_bytes >= 10_000_000f64 {
        // 99.9MB/s:dn
        print!(" {:4.1}MB/s:dn", rate_down_bytes / 1_000_000f64);
    } else if rate_down_bytes >= 1_000_000f64 {
        // 9.99MB/s:dn
        print!(" {:4.2}MB/s:dn", rate_down_bytes / 1_000_000f64);
    } else if rate_down_bytes >= 100_000f64 {
        // " 100KB/s:dn"
        print!(" {:4.0}KB/s:dn", rate_down_bytes / 1_000f64);
    } else if rate_down_bytes >= 10_000f64 {
        // 99.9KB/s:dn
        print!(" {:4.1}KB/s:dn", rate_down_bytes / 1_000f64);
    } else {
        // 99999B/s:dn
        print!(" {:5.0}B/s:dn", rate_down_bytes);
    }

    // cores used
    if iter_cores >= 100f32 {
        // " 100cores"
        println!(" {:4.0}cores", iter_cores);
    } else if iter_cores >= 10f32 {
        // 99.9cores
        println!(" {:4.1}cores", iter_cores);
    } else {
        // 9.99cores
        println!(" {:4.2}cores", iter_cores);
    }
}

fn compute_stats(
    job_cpu: &ProcPidStats,
    iter_counter: LocalTotals,
    latest_success: &LocalTotals,
    latest_client_fail: &LocalTotals,
    latest_server_fail: &LocalTotals,
    latest_conn_error: &LocalTotals,
) -> LocalTotals {
    let now = Utc::now();
    let end_cpu = cputime::cpu();
    let diff_cpu = end_cpu - iter_counter.cpu;
    let iter_runtime = diff_cpu.elapsed;
    // If the time since the previous report was too short, skip this one.
    if iter_runtime.as_millis() < 500 {
        return iter_counter;
    }

    let runtime = end_cpu.elapsed - job_cpu.elapsed;
    let end_error_count =
        latest_client_fail.count + latest_server_fail.count + latest_conn_error.count;
    let end_counter = LocalTotals {
        count: latest_success.count + end_error_count,
        bytes_up: latest_success.bytes_up
            + latest_client_fail.bytes_up
            + latest_server_fail.bytes_up
            + latest_conn_error.bytes_up,
        bytes_down: latest_success.bytes_down
            + latest_client_fail.bytes_down
            + latest_server_fail.bytes_down
            + latest_conn_error.bytes_down,
        elapsed: latest_success.elapsed
            + latest_client_fail.elapsed
            + latest_server_fail.elapsed
            + latest_conn_error.elapsed,
        error_count: end_error_count,
        cpu: end_cpu,
    };
    let num_calls = end_counter.count - iter_counter.count;
    let num_errs = end_counter.error_count - iter_counter.error_count;
    let calls_ms_total = end_counter.elapsed - iter_counter.elapsed;
    let bytes_up = end_counter.bytes_up - iter_counter.bytes_up;
    let bytes_down = end_counter.bytes_down - iter_counter.bytes_down;
    // let mem = memory_stats::memory_stats().unwrap();
    report_stats(
        now,
        runtime,
        iter_runtime,
        calls_ms_total,
        num_calls,
        num_errs,
        bytes_up,
        bytes_down,
        diff_cpu.cpu_cores(),
    );
    end_counter
}

fn stats(rx: Receiver<()>, counters: Arc<TotalsSet>, stat_period: Duration) {
    let start_dt = Utc::now();
    let job_cpu = cputime::cpu();
    // Combine error and success totals for display
    let mut iter_counter = LocalTotals {
        bytes_up: 0,
        bytes_down: 0,
        count: 0,
        elapsed: 0,
        error_count: 0,
        cpu: job_cpu.clone(),
    };
    loop {
        match rx.recv_timeout(stat_period) {
            Err(RecvTimeoutError::Timeout) => {
                let (success, client_fail, server_fail, conn_error) = counters.into_local();
                iter_counter = compute_stats(
                    &job_cpu,
                    iter_counter,
                    &success,
                    &client_fail,
                    &server_fail,
                    &conn_error,
                );
            }
            Err(_) => {
                let (success, client_fail, server_fail, conn_error) = counters.into_local();
                compute_stats(
                    &job_cpu,
                    iter_counter,
                    &success,
                    &client_fail,
                    &server_fail,
                    &conn_error,
                );
                break;
            }
            Ok(_) => eprintln!("Unexpected message sent to stats thread. Ignoring."),
        }
    }
    let end_cpu = cputime::cpu();
    let end_dt = Utc::now();
    let total_cpu = end_cpu - job_cpu;
    let total_runtime_sec = total_cpu.elapsed.as_secs_f64();
    let (success, client_fail, server_fail, conn_error) = counters.into_local();

    fn byte_format(bytes: u64) -> String {
        if bytes < 100_000 {
            format!("{:>8}     bytes", bytes)
        } else if bytes < 100_000_000 {
            format!("{:>12.3} KB", bytes as f64 / 1000f64)
        } else if bytes < 100_000_000_000 {
            format!("{:>12.3} MB", bytes as f64 / 1_000_000f64)
        } else if bytes < 100_000_000_000_000 {
            format!("{:>12.3} GB", bytes as f64 / 1_000_000_000f64)
        } else {
            format!("{:>12.3} TB", bytes as f64 / 1_000_000_000_000f64)
        }
    }

    fn byte_format_f64(bytes: f64) -> String {
        if bytes < 100_000f64 {
            format!("{:>12.3} bytes", bytes)
        } else if bytes < 100_000_000f64 {
            format!("{:>12.3} KB", bytes as f64 / 1000f64)
        } else if bytes < 100_000_000_000f64 {
            format!("{:>12.3} MB", bytes as f64 / 1_000_000f64)
        } else if bytes < 100_000_000_000_000f64 {
            format!("{:>12.3} GB", bytes as f64 / 1_000_000_000f64)
        } else {
            format!("{:>12.3} TB", bytes as f64 / 1_000_000_000_000f64)
        }
    }

    // TODO: compute_stats should not output LocalTotals. It should be something that supports the below.
    println!("Results:");
    println!(
        "Started:               {}",
        start_dt.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    println!(
        "Finished:              {}",
        end_dt.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    println!("Elapsed time:            {:>12.3} s", total_runtime_sec);
    let total_requests = success.count + client_fail.count + server_fail.count + conn_error.count;
    println!("Total requests:          {:>8}", total_requests);
    println!(
        "Total good requests:     {:>8}     ({:>5.1}%)",
        success.count,
        success.count as f64 / total_requests as f64 * 100f64
    );
    println!(
        "Total 4xx requests:      {:>8}     ({:>5.1}%)",
        client_fail.count,
        client_fail.count as f64 / total_requests as f64 * 100f64
    );
    println!(
        "Total 5xx requests:      {:>8}     ({:>5.1}%)",
        server_fail.count,
        server_fail.count as f64 / total_requests as f64 * 100f64
    );
    println!(
        "Total connection errors: {:>8}     ({:>5.1}%)",
        conn_error.count,
        conn_error.count as f64 / total_requests as f64 * 100f64
    );
    let total_request_sec =
        (success.elapsed + client_fail.elapsed + server_fail.elapsed + conn_error.elapsed) as f64
            / 1000f64;
    println!(
        "Total request time:      {:>12.3} seconds",
        total_request_sec
    );
    let total_data_up =
        success.bytes_up + client_fail.bytes_up + server_fail.bytes_up + conn_error.bytes_up;
    println!("Total data uploaded:     {}", byte_format(total_data_up));
    let total_data_down = success.bytes_down
        + client_fail.bytes_down
        + server_fail.bytes_down
        + conn_error.bytes_down;
    println!("Total data downloaded:   {}", byte_format(total_data_down));
    println!(
        "Request rate:            {:>12.3} req/sec",
        total_requests as f64 / total_runtime_sec
    );
    println!(
        "Upload rate:             {}/sec",
        byte_format_f64(total_data_up as f64 / total_runtime_sec)
    );
    println!(
        "Download rate:           {}/sec",
        byte_format_f64(total_data_down as f64 / total_runtime_sec)
    );
    println!(
        "Request concurrency:     {:>12.3}",
        total_request_sec / total_runtime_sec
    );
    println!(
        "Avg request time:        {:>12.3} ms",
        total_request_sec * 1000f64 / total_requests as f64
    );
    println!(
        "CPU used                 {:>12.3} cores",
        (total_cpu.system + total_cpu.user).div_duration_f64(total_cpu.elapsed)
    );
    debug!("Stats complete.");
}

fn wait_for_start(start_at: Option<DateTime<FixedOffset>>, cancel_rx: &Receiver<()>) {
    if let Some(start_at) = start_at {
        let seconds_away = (start_at - Utc::now().fixed_offset()).num_seconds();
        let hours_part = seconds_away / 3600;
        let seconds_part = seconds_away % 3600;
        let minutes_part = seconds_part / 60;
        let seconds_part = seconds_part % 60;
        if hours_part > 0 {
            eprintln!(
                "Starting at {} in {}h{}m{}s.",
                start_at, hours_part, minutes_part, seconds_part
            );
        } else if minutes_part > 0 {
            eprintln!(
                "Starting at {} in {}m{}s.",
                start_at, minutes_part, seconds_part
            );
        } else if seconds_part > 0 {
            eprintln!("Starting at {} in {}s.", start_at, seconds_part);
        } else {
            eprintln!(
                "Scheduled to start at {} (in the past). Starting job NOW.",
                start_at
            );
            return;
        }
        let ms_away = (start_at - Utc::now().fixed_offset()).num_milliseconds() - 10_000;
        let mut cancel = false;
        if ms_away > 0 {
            let ms_delay = Duration::from_millis(ms_away as u64);
            // If, while waiting, ctrl+c is pressed, then just start running the job.
            cancel = cancel_rx.recv_timeout(ms_delay).is_ok();
        }
        loop {
            let ms_away = (start_at - Utc::now().fixed_offset()).num_milliseconds();
            if ms_away > 0 && !cancel {
                eprint!("{}...", f64::round(ms_away as f64 / 1000 as f64));
                let ms_delay = Duration::from_millis(min(ms_away as u64, 1000));
                cancel = cancel_rx.recv_timeout(ms_delay).is_ok();
            } else {
                eprintln!();
                break;
            }
        }
    }
    eprintln!("Starting job NOW.");
}

pub fn run_traffic(config: Configuration, urls: Receiver<UrlEntry>) {
    // TODO: A better way to do an end-of-message signal is a completely different channel,
    // or use an enum.
    let stop_logging = Sample {
        timestamp: DateTime::from_timestamp_nanos(12344321),
        elapsed: Duration::MAX,
        success: true,
        response_code: "__END__".to_string(),
        bytes_up: 12344321,
        bytes_down: 12344321,
        method: "GET".to_string(),
        url: "hmm".to_string(),
        thread: 0,
        threads: 0,
    };

    let num_threads = if config.threads == 0 {
        std::thread::available_parallelism().map_or(1, |t| t.get())
    } else {
        config.threads
    };
    let run_length = config.time;

    // Add the Connection header according to connection config, if the header isn't already there.
    let mut headers = config.headers.clone();
    if headers
        .iter()
        .find(|header| header.to_ascii_lowercase().starts_with("connection:"))
        .is_none()
    {
        if config.connection.eq_ignore_ascii_case("keep-alive") {
            headers.push("Connection: keep-alive".to_owned());
        } else {
            headers.push("Connection: close".to_owned());
        }
    }

    eprintln!("Using {} threads.", num_threads);

    scope(|scope| {
        let (tx, rx) = bounded(1);
        let (result_tx, result_rx) = bounded(1000);
        let (stat_tx, stat_rx) = bounded(0);
        let (cancel_tx, cancel_rx) = bounded(0);
        let counters = Arc::new(TotalsSet {
            success: Totals {
                count: AtomicU64::new(0),
                bytes_up: AtomicU64::new(0),
                bytes_down: AtomicU64::new(0),
                elapsed: AtomicU64::new(0),
            },
            client_fail: Totals {
                count: AtomicU64::new(0),
                bytes_up: AtomicU64::new(0),
                bytes_down: AtomicU64::new(0),
                elapsed: AtomicU64::new(0),
            },
            server_fail: Totals {
                count: AtomicU64::new(0),
                bytes_up: AtomicU64::new(0),
                bytes_down: AtomicU64::new(0),
                elapsed: AtomicU64::new(0),
            },
            conn_error: Totals {
                count: AtomicU64::new(0),
                bytes_up: AtomicU64::new(0),
                bytes_down: AtomicU64::new(0),
                elapsed: AtomicU64::new(0),
            },
        });

        let mut builder = Client::builder().user_agent(config.user_agent);
        if !config.timeout.is_zero() {
            builder = builder.timeout(config.timeout);
        } else {
            builder = builder.timeout(None);
        }
        if config.identity_pem.is_some() {
            builder = builder
                .use_rustls_tls()
                .identity(config.identity_pem.unwrap());
        }
        if config.insecure {
            builder = builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }

        if !config.connection.eq_ignore_ascii_case("keep-alive") {
            builder = builder.pool_max_idle_per_host(0);
        }

        let client = builder.build().unwrap();

        let mut threads = Vec::with_capacity(num_threads);

        // Spin up traffic generators
        for thread in 0..num_threads {
            let thread_rx = rx.clone();
            let thread_client = client.clone();
            let thread_result_tx = result_tx.clone();
            let thread_baseurl = config.baseurl.clone();
            let thread_base_headers = headers.clone();
            let handle = scope.spawn(move || {
                traffic_user(
                    thread_baseurl,
                    thread_base_headers,
                    thread,
                    num_threads,
                    thread_rx,
                    thread_client,
                    thread_result_tx,
                )
            });
            threads.push(handle);
        }

        // Spin up logger outputter
        let counters_a = counters.clone();
        let logger_thread = scope.spawn(|| logger(result_rx, counters_a));
        ctrlc::set_handler(move || cancel_tx.send(()).expect("Unable to send cancel signal."))
            .expect("Could not set up Ctrl-C handler.");

        // If job must start at a specifc time, wait until then.
        wait_for_start(config.start_at, &cancel_rx);

        // Spin up cpu and mem stats outputter
        let counters_b = counters.clone();
        let stats_thread = scope.spawn(|| stats(stat_rx, counters_b, config.stat_period));

        // Send traffic
        let start = Instant::now();
        let mut delay_total: Duration = Duration::ZERO;
        let deadline = start + run_length;
        let start_cpu = cputime::cpu();
        let mut cancel = false;
        while start.elapsed() < run_length && !cancel {
            select! {
                recv(urls) -> url_entry => {
                    if url_entry.is_err() {
                        // TODO: start again until time is done.
                        eprintln!("Reached the end of the urls list before the time limit was reached.");
                        break;
                    }
                    let url_entry = url_entry.ok();

                    // I'm sure time will show that this is not ideal: delay is calculated by summing up
                    // the total expected delay thus far, find the difference compared to the elapsed
                    // time, and if greater than 0, sleep.
                    // The expected problem is that if a network hiccup occurs, it *could* cause a ton of
                    // calls to "bunch up" after the hiccup completes, sending a swarm ASAP until delay
                    // catches up again.
                    let call_delay = url_entry.as_ref().unwrap().delay;
                    delay_total += call_delay.clone();
                    let delay = delay_total
                        .checked_sub(start.elapsed())
                        .unwrap_or(Duration::ZERO);
                    if delay > Duration::ZERO {
                        // If a cancel signal is received while waiting, then stop waiting
                        // and start the canceling process.
                        cancel = cancel_rx.recv_timeout(delay).is_ok();
                    }

                    // Send the request (if we're not canceling the operation)
                    if !cancel {
                        tx.send_deadline(url_entry, deadline).unwrap_or_default();
                    }
                },
                recv(cancel_rx) -> _ => {
                    eprintln!("Received request to quit early.");
                    cancel = true;
                },
            }
        }
        eprintln!("Shutting down. Waiting for active requests to finish.");
        debug!("Shutting down threads.");
        // Signal to all traffic generator threads to shutdown
        for _ in 0..num_threads {
            // TODO: Make the shutdown signal in a different channel, because when it is in
            // the same channel, it is possible for the requests to get "backed up".
            tx.send(None).unwrap();
        }

        let end_cpu = cputime::cpu(); // Capture CPU as soon as job completes.

        // Signal to stats thread to shutdown
        drop(stat_tx);
        if let Err(e) = stats_thread.join() {
            eprintln!(
                "Failed to signal the stats thread to stop gracefully. Error: {:?}",
                e
            );
        }

        // Collect stats
        let total = threads
            .into_iter()
            .map(|t| t.join().unwrap_or(0))
            .fold(0, |acc, x| acc + x);

        // Signal to logger thread to shutdown
        if let Err(e) = result_tx.send(stop_logging) {
            eprintln!(
                "Failed to signal the logger to stop gracefully. Error: {:?}",
                e
            );
        } else if let Err(e) = logger_thread.join() {
            eprintln!("Failed to join logger thread. Error: {:?}", e);
        }

        if log_enabled!(Level::Debug) {
            let diff_cpu = end_cpu - start_cpu;
            debug!(
                "After {}ms, made {} calls. User: {}ms System: {}ms",
                diff_cpu.elapsed.as_millis(),
                total,
                diff_cpu.user.as_millis(),
                end_cpu.system.as_millis()
            );
        }
    });
}
