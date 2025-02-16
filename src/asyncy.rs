use async_channel::{bounded, Receiver, Sender};
use futures::{stream, StreamExt};
use rand::{self, Rng};
use reqwest::Client;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio;

const CONCURRENT_REQUESTS: usize = 1000;

async fn sendie(series_id: usize, s: Sender<u64>) -> usize {
    let start = std::time::Instant::now();
    let target_duration = std::time::Duration::from_secs(10);

    let bodies = stream::iter(0..CONCURRENT_REQUESTS)
        .map(|user_id| {
            // let user_id = user_ids.fetch_add(1, Ordering::SeqCst);
            // let client = &client;
            async move {
                let mut num_things = 0;
                while start.elapsed() < target_duration {
                    let delay = { rand::rng().random_range(900..1100) };
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    num_things += 1;
                    // eprintln!(
                    //     "After waiting {}ms, user {} did a thing: Its total: {} things.",
                    //     delay, user_id, num_things
                    // );
                    // let resp = client.get(url).send().await.unwrap();
                    // resp.bytes().await.unwrap();
                }
                num_things
            }
        })
        .buffer_unordered(CONCURRENT_REQUESTS);

    bodies.fold(0, |acc, x| async move { acc + x }).await
    // bodies
    //     .for_each(|_| async {
    //         // match b {
    //         //     Ok(b) => println!("Got {} bytes", b.len()),
    //         //     Err(e) => eprintln!("Got an error: {}", e),
    //         // }
    //     })
    //     .await;
}

async fn recvie(series_id: usize, r: Receiver<u64>) -> usize {
    let start = std::time::Instant::now();
    let target_duration = std::time::Duration::from_secs(10);
    let client = Client::new();

    let urls = vec!["http://localhost:8080"; CONCURRENT_REQUESTS];
    let user_ids = Arc::new(AtomicI32::new(0));

    let bodies = stream::iter(0..CONCURRENT_REQUESTS)
        .map(|user_id| {
            // let user_id = user_ids.fetch_add(1, Ordering::SeqCst);
            // let client = &client;
            async move {
                let mut num_things = 0;
                while start.elapsed() < target_duration {
                    let delay = { rand::rng().random_range(900..1100) };
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    num_things += 1;
                    // eprintln!(
                    //     "After waiting {}ms, user {} did a thing: Its total: {} things.",
                    //     delay, user_id, num_things
                    // );
                    // let resp = client.get(url).send().await.unwrap();
                    // resp.bytes().await.unwrap();
                }
                num_things
            }
        })
        .buffer_unordered(CONCURRENT_REQUESTS);

    bodies.fold(0, |acc, x| async move { acc + x }).await
    // bodies
    //     .for_each(|_| async {
    //         // match b {
    //         //     Ok(b) => println!("Got {} bytes", b.len()),
    //         //     Err(e) => eprintln!("Got an error: {}", e),
    //         // }
    //     })
    //     .await;
}

pub async fn run_traffic() {
    let num_workers = tokio::runtime::Handle::current().metrics().num_workers();
    eprintln!("number of workers: {}", num_workers);
    let start = std::time::Instant::now();

    let (s, r) = bounded::<u64>(100);
    let mut workers = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let sc = s.clone();
        let rc = r.clone();
        workers.push(tokio::spawn(async move { sendie(i, sc).await }));
        workers.push(tokio::spawn(async move { recvie(i, rc).await }));
    }

    let mut total = 0;
    for worker in &mut workers {
        total += worker.await.unwrap();
    }
    let total_time = start.elapsed();
    eprintln!(
        "After {:?}ms, {} workers completed {} things total.",
        total_time.as_millis(),
        &workers.len(),
        total
    );
}
