use crossbeam::channel::{bounded, Receiver};
use reqwest::blocking::Client;
use reqwest::StatusCode;
use std::thread::{scope, sleep};
use std::time::{Duration, Instant};

struct CallResult {
    status: StatusCode,
    bytes_received: usize,
}

// impl From

fn hit_target(client: &Client) -> Result<CallResult, reqwest::Error> {
    let req = client.get("http://127.0.0.1:8080/logs").send()?;
    let status = req.status();
    let len = req.text()?.len();

    Ok(CallResult {
        status,
        bytes_received: len,
    })
}

fn traffic_user(rx: Receiver<u32>, client: Client) -> usize {
    let mut count = 0;
    let mut succeess_count = 0;
    let mut total_bytes = 0;
    let mut total_duration = Duration::ZERO;
    let mut conn_error_count = 0;
    let mut conn_error_duration = Duration::ZERO;

    loop {
        let item = rx.recv().unwrap_or(0);
        if item == 0 {
            // Got the signal to quit
            // (Ideally, this'd be a watch or broadcaster and we'd select
            // between that receiver and the "main" receiver, but this'll do)
            break;
        }
        let start = Instant::now();
        match hit_target(&client) {
            Result::Ok(v) => {
                total_duration += start.elapsed();
                count += 1;
                total_bytes += v.bytes_received;
                if v.status.is_success() {
                    succeess_count += 1;
                }
            }
            Result::Err(_) => {
                conn_error_count += 1;
                conn_error_duration += start.elapsed();
            }
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

pub fn run_traffic() {
    let num_threads = std::thread::available_parallelism().map_or(1, |t| t.get());
    // let num_threads = 1;
    let run_length = Duration::from_secs(10);
    let calls_per_minute = 6000_f64;
    let call_delay = if calls_per_minute != 0_f64 {
        Duration::from_secs_f64(60_f64 / calls_per_minute)
    } else {
        Duration::ZERO
    };
    eprintln!("Using {} threads.", num_threads);

    scope(|scope| {
        let (tx, rx) = bounded(1); // TODO: Make this big again when the main stream isn't used for shutdown as well.
        let client = Client::builder().build().unwrap();

        let mut threads = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let thread_rx = rx.clone();
            let thread_client = client.clone();
            let handle = scope.spawn(|| traffic_user(thread_rx, thread_client));
            threads.push(handle);
        }
        // Send traffic
        let mut i: u32 = 0; // Must be u32 for delay calc for now, so has 4b call limit
        let start = Instant::now();
        let deadline = start + run_length;
        while start.elapsed() < run_length {
            i += 1;
            let delay = (call_delay * i)
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);
            if delay > Duration::ZERO {
                sleep(delay);
            }
            tx.send_deadline(i, deadline).unwrap_or_default();
        }
        // Signal to all threads to shutdown
        for _ in 0..num_threads {
            // TODO: Make the shutdown signal in a different channel, because when it is in
            // the same channel, it is possible for the requests to get "backed up".
            tx.send(0).unwrap();
        }
        // Collect stats
        let total = threads
            .into_iter()
            .map(|t| t.join().unwrap_or(0))
            .fold(0, |acc, x| acc + x);
        let total_elapsed = start.elapsed();

        eprintln!(
            "After {}ms, made {} calls.",
            total_elapsed.as_millis(),
            total
        );
    });
}
