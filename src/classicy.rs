use crossbeam::channel::{bounded, Receiver};
use reqwest::blocking::Client;
use std::thread::{scope, sleep};
use std::time::{Duration, Instant};

fn hit_target(client: &Client) -> usize {
    // sleep(Duration::from_millis(200));
    let text = client
        .get("http://127.0.0.1:8080/logs")
        .send()
        .unwrap()
        .text()
        .unwrap();

    text.len()
}

fn traffic_user(rx: Receiver<u64>, client: Client) -> usize {
    let mut count = 0;
    let mut total_bytes = 0;
    let mut total_duration = Duration::ZERO;
    loop {
        let item = rx.recv().unwrap_or(0);
        if item == 0 {
            // Got the signal to quit
            // (Ideally, this'd be a watch or broadcaster and we'd select
            // between that receiver and the "main" receiver, but this'll do)
            break;
        }
        let start = Instant::now();
        total_bytes += hit_target(&client);
        total_duration += start.elapsed();
        count = count + 1;
    }
    eprintln!(
        "Thread made calls={} total_bytes={} total_duration_millis={}",
        count,
        total_bytes,
        total_duration.as_millis()
    );
    count
}

pub fn run_traffic() {
    // let num_threads = std::thread::available_parallelism().map_or(1, |t| t.get());
    let num_threads = 1;
    eprintln!("Using {} threads.", num_threads);

    scope(|scope| {
        let (tx, rx) = bounded(1); // TODO: Make this big again when the main stream isn't used for shutdown as well.
        let start = Instant::now();
        let client = Client::builder().build().unwrap();

        let mut threads = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let thread_rx = rx.clone();
            let thread_client = client.clone();
            let handle = scope.spawn(|| traffic_user(thread_rx, thread_client));
            threads.push(handle);
        }
        // Send traffic
        let mut i = 0;
        let deadline = start + Duration::from_secs(10);
        while start.elapsed() < Duration::from_secs(10) {
            i += 1;
            // sleep(Duration::from_secs(1)); // traffic rate
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
            "After {}ms, ran {} times.",
            total_elapsed.as_millis(),
            total
        );
    });
}
