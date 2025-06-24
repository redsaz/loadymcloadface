use config::{Config, ConfigError, Environment, File};
use core::result::Result;
use dotenv::dotenv;
use reqwest::{Identity, Url};
use std::env;
use std::io::Read;
use std::time::Duration;

/// Configuration for running a load test.
#[derive(/*Serialize, Deserialize, */ Debug)]
pub struct Configuration {
    /// If true then print debug info
    pub debug: bool,
    /// The urls file may be a list of paths, which are tacked on to the end of this base URL.
    pub baseurl: Url,
    /// Whether to have a connection keep-alive or close after every http transaction.
    /// keep-alive will use connection pools, and add a "Connection: keep-alive" header.
    /// close will turn off connection pools, and adds a "Connection: close" header.
    pub connection: String,
    /// A list of headers that are provided for each request.
    pub headers: Vec<String>,
    /// The maximum number of threads (simultaneous users) that will run. If 0, native number of
    /// cores is used.
    pub threads: usize,
    /// The target traffic rate to simulate. Can be specified in calls per second, minute, or
    /// hour with the suffixes "/s", "/m", or "/h" respectively. Example: "10/s" will target 10
    /// calls a second, or 0.1/m will target 0.1 calls a minute (or 6 calls an hour). If no unit is
    /// specified, calls per second is assumed. Defaults to 1 call a second.
    pub rate: f64,
    /// node identifier, ranging from 1 to nodes. Typically, the app will run through each url
    /// entry. This is fine when running a single instance of the app, but when running multiple
    /// instances of the app concurrently, it may be helpful for the instances to divide the url
    /// list across all the instances so that they do not duplicate the list.
    pub node: usize,
    /// Total number of nodes, default of 1. See the `node` documentation for more info on how this
    /// divides the url list between the various instances of the app.
    pub nodes: usize,
    /// How long to run the load test. Can be specified in seconds, minutes, or hours with the
    /// suffixes "s", "m", or "h" respectively. Example: "10s" will run the test for 10 seconds.
    /// If no unit is specified, seconds is assumed. Defaults to 5 minutes.
    pub time: Duration,
    /// How long to wait for the entirety of connecting, writing, and reading before closing.
    /// Specified in milliseconds, seconds, or minutes with the suffixes "ms", "s", or "m".
    /// If 0, there is no timeout. Defaults to 30 seconds.
    pub timeout: Duration,
    /// Pass a client cert for each request for mTLS, if defined with the path to a pem file that
    /// contains both the cert and key. By default this is undefined.
    pub identity_pem: Option<Identity>,
    /// How often to report the run's statistics. Defaults to 10 seconds.
    pub stat_period: Duration,
    /// The user agent to send with each request.
    pub user_agent: String,
}

fn rate_from_string(rate_str: &str) -> Result<f64, ConfigError> {
    let unit =
        rate_str.find(|c: char| c == '/' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
    let (unit, amount) = if let Some(idx) = unit {
        (&rate_str[idx..], rate_str[..idx].parse::<f64>().unwrap())
    } else {
        ("s", rate_str.parse::<f64>().unwrap())
    };

    match unit {
        "ms" | "/ms" => Ok(amount * 1000.0),
        "s" | "/s" => Ok(amount),
        "m" | "/m" => Ok(amount / 60.0),
        "h" | "/h" => Ok(amount / 3600.0),
        _ => Err(ConfigError::Message(
            "Unknown unit specified for rate.".to_string(),
        )),
    }
}

fn time_from_string(time_str: &str) -> Result<Duration, ConfigError> {
    let unit =
        time_str.find(|c: char| c == '/' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'));
    let (unit, amount) = if let Some(idx) = unit {
        (&time_str[idx..], time_str[..idx].parse::<f64>().unwrap())
    } else {
        ("s", time_str.parse::<f64>().unwrap())
    };

    match unit {
        "ms" => Ok(Duration::from_secs_f64(amount / 1000.0)),
        "s" => Ok(Duration::from_secs_f64(amount)),
        "m" => Ok(Duration::from_secs_f64(amount * 60.0)),
        "h" => Ok(Duration::from_secs_f64(amount * 3600.0)),
        _ => Err(ConfigError::Message(
            "Unknown unit specified for duration.".to_string(),
        )),
    }
}

fn identity_from_string(pem_file_str: Option<&str>) -> Option<Identity> {
    if pem_file_str.is_none() {
        return Option::None;
    }
    let mut buf = Vec::new();
    std::fs::File::open(pem_file_str.unwrap())
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    Some(reqwest::Identity::from_pem(&buf).unwrap())
}

/// Configuration for this app.
///
/// Configuration is loaded in the following order, highest priority to lowest:
///  - environment variables
///  - environment variables in .env.toml file in current working directory
///  - Config file specified by LOADY_CONFIG environment variable
///  - Config file loady.toml in current working directory
///
/// # Errors
///
/// This [`core::result::Result`] will be an [`Err`] if some IO error occurs
/// during loading or if some required values were not provided
pub fn config() -> Result<Configuration, ConfigError> {
    dotenv().ok(); // Load .env entries into env vars
    let config_file = "loady.toml";
    let custom_config = env::var("LOADY_CONFIG").unwrap_or(config_file.to_owned());

    let s = Config::builder()
        .add_source(File::with_name(config_file).required(false))
        .add_source(File::with_name(custom_config.as_str()).required(false))
        .add_source(
            Environment::with_prefix("loady")
                .list_separator("(header)")
                .with_list_parse_key("headers")
                .try_parsing(true),
        )
        .set_default("debug", "false")?
        .set_default("baseurl", "http://localhost/")?
        .set_default("threads", 0)?
        .set_default("rate", "1s")?
        .set_default("node", 1)?
        .set_default("nodes", 1)?
        .set_default("time", "1m")?
        .set_default("timeout", "30")?
        .set_default("connection", "keep-alive")?
        .set_default("headers", Vec::<String>::new())?
        .set_default("identity_pem", Option::None::<String>)?
        .set_default("stat_period", "10s")?
        .set_default(
            "user_agent",
            format!("loadymcloadface/{}", env!("CARGO_PKG_VERSION")),
        )?
        .build()?;

    if s.get_bool("debug").unwrap_or_default() {
        eprintln!("debug: {:?}", s.get_bool("debug"));
        eprintln!("baseurl: {:?}", s.get::<String>("baseurl"));
        eprintln!("threads: {:?}", s.get_int("threads"));
        eprintln!("rate: {:?}", s.get::<String>("rate"));
        eprintln!("node: {:?}", s.get_int("node"));
        eprintln!("nodes: {:?}", s.get_int("nodes"));
        eprintln!("time: {:?}", s.get::<String>("time"));
        eprintln!("timeout: {:?}", s.get::<String>("timeout"));
        eprintln!("connection: {:?}", s.get::<String>("connection"));
        eprintln!("headers: {:?}", s.get_array("headers"));
        eprintln!(
            "identity_pem: {:?}",
            s.get::<Option<String>>("identity_pem")
        );
        eprintln!("stat_period: {:?}", s.get::<String>("stat_rate"));
    }

    Ok(Configuration {
        debug: s.get("debug").unwrap(),
        baseurl: Url::parse(s.get_string("baseurl").unwrap().as_str()).unwrap(),
        connection: s.get("connection").unwrap(),
        headers: s.get("headers").unwrap(),
        threads: s.get("threads").unwrap(),
        rate: rate_from_string(&s.get_string("rate").unwrap()).unwrap(),
        node: s.get("node").unwrap(),
        nodes: s.get("nodes").unwrap(),
        time: time_from_string(&s.get_string("time").unwrap()).unwrap(),
        timeout: time_from_string(&s.get_string("timeout").unwrap()).unwrap(),
        identity_pem: identity_from_string(s.get("identity_pem").unwrap()),
        stat_period: time_from_string(&s.get_string("stat_period").unwrap()).unwrap(),
        user_agent: s.get("user_agent").unwrap(),
    })
}
