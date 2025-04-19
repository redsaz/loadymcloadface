use crate::configuration::Configuration;
use crate::siegeurls::{SiegeUrls, UrlEntry};
use chrono::{DateTime, Utc};
use crossbeam::channel::{bounded, Receiver, Sender};
use reqwest::blocking::Client;
use reqwest::{Method, StatusCode, Url};
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread::{scope, sleep};
use std::time::{Duration, Instant};

struct CallResult {
    status: StatusCode,
    bytes_received: usize,
}

// useful jmeter values:
// "timeStamp","elapsed","label","responseCode","threadName","success","bytes","allThreads"
#[derive(Debug)]
struct Sample {
    timestamp: DateTime<Utc>,
    elapsed: Duration,
    success: bool,
    response_code: String,
    bytes: u64,
    method: String,
    url: String,
    thread: usize,
    threads: usize,
}

impl fmt::Display for Sample {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{},{},{}",
            self.timestamp.timestamp_millis(),
            self.elapsed.as_millis(),
            self.success,
            self.response_code,
            self.bytes,
            self.method,
            self.url,
            self.thread,
            self.threads
        )
    }
}

fn hit_target(client: &Client, url: &str, method: Method) -> Result<CallResult, reqwest::Error> {
    // "http://127.0.0.1:8080/logs"
    let req = client.request(method, url).send()?;
    let status = req.status();
    let len = req.text()?.len();

    Ok(CallResult {
        status,
        bytes_received: len,
    })
}

fn traffic_user(
    baseurl: Url,
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
        let url = &baseurl.join(&url_entry.urlpart.clone()).unwrap();
        let entry = match hit_target(&client, url.as_str(), url_entry.method.clone()) {
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
                    bytes: v.bytes_received as u64,
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
                    bytes: 0,
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

fn logger(rx: Receiver<Sample>) {
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
        count += 1;
        writeln!(log, "{}", entry).unwrap();
    }
    log.flush().unwrap();
    eprintln!("Logging complete. Received {} entries.", count);
}

pub fn run_traffic(config: Configuration, urls: Receiver<UrlEntry>) {
    // TODO: A better way to do an end-of-message signal is a completely different channel,
    // or use an enum.
    let stop_logging = Sample {
        timestamp: DateTime::from_timestamp_nanos(12344321),
        elapsed: Duration::MAX,
        success: true,
        response_code: "__END__".to_string(),
        bytes: 12344321,
        method: "GET".to_string(),
        url: "hmm".to_string(),
        thread: 0,
        threads: 0,
    };

    let num_threads = std::thread::available_parallelism().map_or(1, |t| t.get());
    let run_length = config.time;

    eprintln!("Using {} threads.", num_threads);

    scope(|scope| {
        let (tx, rx) = bounded(1); // TODO: Make this big again when the main stream isn't used for shutdown as well.
        let (result_tx, result_rx) = bounded(1000);

        let mut builder = Client::builder();
        if config.timeout.is_some() {
            builder = builder.timeout(config.timeout);
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
            let handle = scope.spawn(move || {
                traffic_user(
                    thread_baseurl,
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
        let logger_thread = scope.spawn(|| logger(result_rx));
        // Send traffic
        let mut i: i64 = 0;
        let start = Instant::now();
        let mut delay_total: Duration = Duration::ZERO;
        let deadline = start + run_length;
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
        // Collect stats
        let total = threads
            .into_iter()
            .map(|t| t.join().unwrap_or(0))
            .fold(0, |acc, x| acc + x);
        let total_elapsed = start.elapsed();

        // Signal to logger thread to shutdown
        if let Err(e) = result_tx.send(stop_logging) {
            eprintln!(
                "Failed to signal the logger to stop gracefully. Error: {:?}",
                e
            );
        } else if let Err(e) = logger_thread.join() {
            eprintln!("Failed to join logger thread. Error: {:?}", e);
        }

        eprintln!(
            "After {}ms, made {} calls.",
            total_elapsed.as_millis(),
            total
        );
    });
}
