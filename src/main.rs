use std::{path::Path, time::Duration};

use loadymcloadface::{configuration, cputime, siegeurls::SiegeUrls};
// use tokio;
//
// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
// async fn main() {
//     loadymcloadface::asyncy::run_traffic().await;
// }

fn main() {
    cputime::init();
    let config = configuration::config().expect("LoadyMcLoadface is misconfigured.");
    let calls_per_sec = config.rate;
    let call_delay = if calls_per_sec != 0_f64 {
        Duration::from_secs_f64(1_f64 / calls_per_sec)
    } else {
        Duration::ZERO
    };
    let stride = config.nodes;
    let offset = config.node - 1;
    if config.debug {
        eprintln!("Call delay is {}ms", call_delay.as_millis());
    }
    let urls =
        SiegeUrls::load_iter_looping_buffered(Path::new("urls.txt"), call_delay, stride, offset);
    eprintln!("urls: {:?}", urls);
    eprintln!("cputime before: {:?}", cputime::cpu());
    loadymcloadface::classicy::run_traffic(config, urls);
    eprintln!("cputime after: {:?}", cputime::cpu());
    // this would be nice output maybe?
    // 00:00:10 | 317 call/s | 65 ms/call | 10 MiB/s | 4.2 %err | 957mcores
    //    total | 412 call/s | 42 ms/call | 15 MiB/s | 3.1 %err | 942mcores
}
