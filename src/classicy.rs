use crate::configuration::Configuration;
use crate::cputime;
use crate::siegeurls::{BodyData, UrlEntry};
use chrono::{DateTime, Utc};
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{StatusCode, Url};
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{scope, sleep};
use std::time::{Duration, Instant};

struct CallResult {
    status: StatusCode,
    bytes_sent: u64,
    bytes_received: u64,
}

struct TotalsSet {
    success: Totals,
    error: Totals,
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
    eprintln!(
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
        let totals = match entry.success {
            true => &counters.success,
            false => &counters.error,
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
    eprintln!("Logging complete. Received {} entries.", count);
}

fn report_stats(
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
    // 0:00:01 1707req/s 1225ms/req 99.0%err 123KB/s:up 123KB/s:dn 21.0cores

    // elapsed job time
    let (h, m, s) = cputime::duration_hms(job_elapsed);
    eprint!("{}:{:02}:{:02}", h, m, s);

    let iter_ms = iter_elapsed.as_millis() as u64;

    // requests per time period
    let reqs_sec = iter_reqs * 1000 / iter_ms;
    if reqs_sec >= 100_000 {
        // " 100kreq/s"
        eprint!("{:4.0}kreq/s", reqs_sec / 1000);
    } else {
        // 99999req/s
        eprint!("{:5.0}req/s", reqs_sec);
    }

    // average response time
    let ms_req = if iter_reqs > 0 {
        iter_req_ms_total as f64 / iter_reqs as f64
    } else {
        0f64
    };
    if ms_req >= 1_000_000f64 {
        // " 1000s/req" (yikes if this happens)
        eprint!(" {:5.0}s/req", ms_req / 1000f64);
    } else if ms_req >= 100_000f64 {
        // 999.9s/req (yikes if this happens)
        eprint!(" {:5.1}s/req", ms_req / 1000f64);
    } else if ms_req >= 10_000f64 {
        // 99.99s/req
        eprint!(" {:5.2}s/req", ms_req / 1000f64);
    } else {
        // 9999ms/req
        eprint!(" {:4.0}ms/req", ms_req);
    }

    // percentage of calls in error
    let err_perc = if iter_reqs > 0 {
        iter_errs as f64 / iter_reqs as f64 * 100f64
    } else {
        0f64
    };
    if err_perc >= 100f64 {
        // " 100%err"
        eprint!(" {:4.0}%err", err_perc);
    } else if err_perc >= 10f64 {
        // 99.9%err
        eprint!(" {:4.1}%err", err_perc);
    } else {
        // 9.99%err
        eprint!(" {:4.2}%err", err_perc);
    }

    // upload rate
    let rate_up_bytes = iter_bytes_up as f64 * 1000f64 / iter_ms as f64;
    if rate_up_bytes >= 100_000_000_000f64 {
        // " 100GB/s:up"
        eprint!(" {:4.0}GB/s:up", rate_up_bytes / 1_000_000_000f64);
    } else if rate_up_bytes >= 10_000_000_000f64 {
        // 99.9GB/s:up
        eprint!(" {:4.1}GB/s:up", rate_up_bytes / 1_000_000_000f64);
    } else if rate_up_bytes >= 1_000_000_000f64 {
        // 9.99GB/s:up
        eprint!(" {:4.2}GB/s:up", rate_up_bytes / 1_000_000_000f64);
    } else if rate_up_bytes >= 100_000_000f64 {
        // " 100MB/s:up"
        eprint!(" {:4.0}MB/s:up", rate_up_bytes / 1_000_000f64);
    } else if rate_up_bytes >= 10_000_000f64 {
        // 99.9MB/s:up
        eprint!(" {:4.1}MB/s:up", rate_up_bytes / 1_000_000f64);
    } else if rate_up_bytes >= 1_000_000f64 {
        // 9.99MB/s:up
        eprint!(" {:4.2}MB/s:up", rate_up_bytes / 1_000_000f64);
    } else if rate_up_bytes >= 100_000f64 {
        // " 100KB/s:up"
        eprint!(" {:4.0}KB/s:up", rate_up_bytes / 1_000f64);
    } else if rate_up_bytes >= 10_000f64 {
        // 99.9KB/s:up
        eprint!(" {:4.1}KB/s:up", rate_up_bytes / 1_000f64);
    } else {
        // 99999B/s:up
        eprint!(" {:5.0}B/s:up", rate_up_bytes);
    }

    // download rate
    let rate_down_bytes = iter_bytes_down as f64 * 1000f64 / iter_ms as f64;
    if rate_down_bytes >= 100_000_000_000f64 {
        // " 100GB/s:dn"
        eprint!(" {:4.0}GB/s:dn", rate_down_bytes / 1_000_000_000f64);
    } else if rate_down_bytes >= 10_000_000_000f64 {
        // 99.9GB/s:dn
        eprint!(" {:4.1}GB/s:dn", rate_down_bytes / 1_000_000_000f64);
    } else if rate_down_bytes >= 1_000_000_000f64 {
        // 9.99GB/s:dn
        eprint!(" {:4.2}GB/s:dn", rate_down_bytes / 1_000_000_000f64);
    } else if rate_down_bytes >= 100_000_000f64 {
        // " 100MB/s:dn"
        eprint!(" {:4.0}MB/s:dn", rate_down_bytes / 1_000_000f64);
    } else if rate_down_bytes >= 10_000_000f64 {
        // 99.9MB/s:dn
        eprint!(" {:4.1}MB/s:dn", rate_down_bytes / 1_000_000f64);
    } else if rate_down_bytes >= 1_000_000f64 {
        // 9.99MB/s:dn
        eprint!(" {:4.2}MB/s:dn", rate_down_bytes / 1_000_000f64);
    } else if rate_down_bytes >= 100_000f64 {
        // " 100KB/s:dn"
        eprint!(" {:4.0}KB/s:dn", rate_down_bytes / 1_000f64);
    } else if rate_down_bytes >= 10_000f64 {
        // 99.9KB/s:dn
        eprint!(" {:4.1}KB/s:dn", rate_down_bytes / 1_000f64);
    } else {
        // 99999B/s:dn
        eprint!(" {:5.0}B/s:dn", rate_down_bytes);
    }

    // cores used
    if iter_cores >= 100f32 {
        // " 100cores"
        eprintln!(" {:4.0}cores", iter_cores);
    } else if iter_cores >= 10f32 {
        // 99.9cores
        eprintln!(" {:4.1}cores", iter_cores);
    } else {
        // 9.99cores
        eprintln!(" {:4.2}cores", iter_cores);
    }
}

fn stats(rx: Receiver<()>, counters: Arc<TotalsSet>) {
    let job_cpu = cputime::cpu();
    let mut iter_cpu = job_cpu.clone();
    // Combine error and success totals for display
    let mut iter_counter = LocalTotals {
        bytes_up: 0,
        bytes_down: 0,
        count: 0,
        elapsed: 0,
    };
    let mut iter_error_count = 0u64;
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Err(RecvTimeoutError::Timeout) => {
                let end_cpu = cputime::cpu();
                let diff_cpu = end_cpu - iter_cpu;
                let iter_runtime = diff_cpu.elapsed;
                iter_cpu = end_cpu;
                let runtime = end_cpu.elapsed - job_cpu.elapsed;
                let end_error_count = counters.error.count.load(Ordering::Relaxed);
                let end_counter = LocalTotals {
                    count: counters.success.count.load(Ordering::Relaxed) + end_error_count,
                    bytes_up: counters.success.bytes_up.load(Ordering::Relaxed)
                        + counters.error.bytes_up.load(Ordering::Relaxed),
                    bytes_down: counters.success.bytes_down.load(Ordering::Relaxed)
                        + counters.error.bytes_down.load(Ordering::Relaxed),
                    elapsed: counters.success.elapsed.load(Ordering::Relaxed)
                        + counters.error.elapsed.load(Ordering::Relaxed),
                };
                let num_calls = end_counter.count - iter_counter.count;
                let num_errs = end_error_count - iter_error_count;
                let calls_ms_total = end_counter.elapsed - iter_counter.elapsed;
                let bytes_up = end_counter.bytes_up - iter_counter.bytes_up;
                let bytes_down = end_counter.bytes_down - iter_counter.bytes_down;
                iter_counter = end_counter;
                iter_error_count = end_error_count;
                // let mem = memory_stats::memory_stats().unwrap();
                report_stats(
                    runtime,
                    iter_runtime,
                    calls_ms_total,
                    num_calls,
                    num_errs,
                    bytes_up,
                    bytes_down,
                    diff_cpu.cpu_cores(),
                );
            }
            Err(_) => {
                eprintln!("Shutting down. Pretend overall stats are printed here.");
                break;
            }
            Ok(_) => eprintln!("Unexpected message sent to stats thread. Ignoring."),
        }
    }
    eprintln!("Stats complete.");
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

    eprintln!("Using {} threads.", num_threads);

    scope(|scope| {
        let (tx, rx) = bounded(1);
        let (result_tx, result_rx) = bounded(1000);
        let (stat_tx, stat_rx) = bounded(0);
        let counters = Arc::new(TotalsSet {
            success: Totals {
                count: AtomicU64::new(0),
                bytes_up: AtomicU64::new(0),
                bytes_down: AtomicU64::new(0),
                elapsed: AtomicU64::new(0),
            },
            error: Totals {
                count: AtomicU64::new(0),
                bytes_up: AtomicU64::new(0),
                bytes_down: AtomicU64::new(0),
                elapsed: AtomicU64::new(0),
            },
        });

        let mut builder = Client::builder();
        if !config.timeout.is_zero() {
            builder = builder.timeout(config.timeout);
        } else {
            builder = builder.timeout(None);
        }
        if config.identity_pem.is_some() {
            builder = builder.identity(config.identity_pem.unwrap());
        }
        let client = builder.build().unwrap();

        let mut threads = Vec::with_capacity(num_threads);

        // Spin up traffic generators
        for thread in 0..num_threads {
            let thread_rx = rx.clone();
            let thread_client = client.clone();
            let thread_result_tx = result_tx.clone();
            let thread_baseurl = config.baseurl.clone();
            let thread_base_headers = config.headers.clone();
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
        // Spin up cpu and mem stats outputter
        let counters_b = counters.clone();
        let stats_thread = scope.spawn(|| stats(stat_rx, counters_b));
        // Send traffic
        let mut i: i64 = 0;
        let start = Instant::now();
        let mut delay_total: Duration = Duration::ZERO;
        let deadline = start + run_length;
        let start_cpu = cputime::cpu();
        while start.elapsed() < run_length {
            i += 1;
            let url_entry = urls.recv();
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
                sleep(delay);
            }

            tx.send_deadline(url_entry, deadline).unwrap_or_default();
        }
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

        let diff_cpu = end_cpu - start_cpu;
        eprintln!(
            "After {}ms, made {} calls. User: {} System: {}",
            diff_cpu.elapsed.as_millis(),
            total,
            diff_cpu.user.as_millis(),
            end_cpu.system.as_millis()
        );
    });
}
