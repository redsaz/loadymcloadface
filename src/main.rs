use std::path::Path;

use loadymcloadface::{configuration, siegeurls};
// use tokio;
//
// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
// async fn main() {
//     loadymcloadface::asyncy::run_traffic().await;
// }

fn main() {
    let config = configuration::config().expect("LoadyMcLoadface is misconfigured.");
    let mut urls = siegeurls::load_iter(Path::new("urls.txt"));
    eprintln!("urls: {:?}", urls);
    loadymcloadface::classicy::run_traffic(config, &mut urls);
}
