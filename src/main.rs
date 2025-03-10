use loadymcloadface::configuration;
// use tokio;
//
// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
// async fn main() {
//     loadymcloadface::asyncy::run_traffic().await;
// }

fn main() {
    let config = configuration::config().expect("LoadyMcLoadface is misconfigured.");
    loadymcloadface::classicy::run_traffic(config);
}
