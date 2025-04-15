use std::{path::Path, time::Duration};

use loadymcloadface::{configuration, siegeurls};
// use tokio;
//
// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
// async fn main() {
//     loadymcloadface::asyncy::run_traffic().await;
// }

fn main() {
    let config = configuration::config().expect("LoadyMcLoadface is misconfigured.");
    let calls_per_sec = config.rate;
    let call_delay = if calls_per_sec != 0_f64 {
        Duration::from_secs_f64(1_f64 / calls_per_sec)
    } else {
        Duration::ZERO
    };
    if config.debug {
        eprintln!("Call delay is {}ms", call_delay.as_millis());
    }
    let mut urls = siegeurls::load_iter(Path::new("urls.txt"), call_delay);
    eprintln!("urls: {:?}", urls);
    loadymcloadface::classicy::run_traffic(config, &mut urls);
}
