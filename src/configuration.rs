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
    /// A list of headers that are provided for each request.
    pub headers: Vec<String>,
    /// The maximum number of threads (simultaneous users) that will run. If 0, native number of
    /// cores is used.
    pub threads: usize,
    /// The target traffic rate to simulate. Can be specified in calls per second, minute, or
    /// hour with the suffixes "s", "m", or "h" respectively. Example: "10s" will target 10
    /// calls a second, or 0.1m will target 0.1 calls a minute (or 6 calls an hour). If no unit is
    /// specified, calls per second is assumed. Defaults to 1 call a second.
    pub rate: f64,
    /// How long to run the load test. Can be specified in seconds, minutes, or hours with the
    /// suffixes "s", "m", or "h" respectively. Example: "10s" will run the test for 10 seconds.
    /// If no unit is specified, seconds is assumed. Defaults to 5 minute.
    pub time: Duration,
    /// How long to wait for the entirety of connecting, writing, and reading before closing,
    /// in seconds. By default the timeout is 30 seconds. If 0, there is no timeout.
    pub timeout: Option<Duration>,
    /// Pass a client cert for each request for mTLS, if defined with the path to a pem file that
    /// contains both the cert and key. By default this is undefined.
    pub identity_pem: Option<Identity>,
}

fn rate_from_string(rate_str: &str) -> Result<f64, ConfigError> {
    let unit = rate_str.to_lowercase().chars().last().unwrap();
    let amount = if unit.is_alphabetic() {
        rate_str[..rate_str.len() - 1]
            .to_string()
            .parse::<f64>()
            .unwrap()
    } else {
        rate_str.parse::<f64>().unwrap()
    };

    match unit {
        's' | '0'..'9' | '.' => Ok(amount),
        'm' => Ok(amount / 60.0),
        'h' => Ok(amount / 3600.0),
        _ => Err(ConfigError::Message(
            "Unknown unit specified for rate.".to_string(),
        )),
    }
}

fn time_from_string(time_str: &str) -> Result<Duration, ConfigError> {
    let unit = time_str.to_lowercase().chars().last().unwrap();
    let amount = if unit.is_alphabetic() {
        time_str[..time_str.len() - 1]
            .to_string()
            .parse::<u64>()
            .unwrap()
    } else {
        time_str.parse::<u64>().unwrap()
    };

    match unit {
        's' | '0'..'9' => Ok(Duration::from_secs(amount)),
        'm' => Ok(Duration::from_secs(amount * 60)),
        'h' => Ok(Duration::from_secs(amount * 3600)),
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
        .set_default("time", "1m")?
        .set_default("timeout", "30")?
        .set_default("headers", Vec::<String>::new())?
        .set_default("identity_pem", Option::None::<String>)?
        .build()?;

    if s.get_bool("debug").unwrap_or_default() {
        println!("debug: {:?}", s.get_bool("debug"));
        println!("baseurl: {:?}", s.get::<String>("baseurl"));
        println!("threads: {:?}", s.get_int("threads"));
        println!("rate: {:?}", s.get::<String>("rate"));
        println!("time: {:?}", s.get::<String>("time"));
        println!("timeout: {:?}", s.get_int("timeout"));
        println!("headers: {:?}", s.get_array("headers"));
        println!(
            "identity_pem: {:?}",
            s.get::<Option<String>>("identity_pem")
        );
    }

    Ok(Configuration {
        debug: s.get("debug").unwrap(),
        baseurl: Url::parse(s.get_string("baseurl").unwrap().as_str()).unwrap(),
        headers: s.get("headers").unwrap(),
        threads: s.get("threads").unwrap(),
        rate: rate_from_string(&s.get_string("rate").unwrap()).unwrap(),
        time: time_from_string(&s.get_string("time").unwrap()).unwrap(),
        timeout: Option::Some(Duration::from_secs(s.get("timeout").unwrap())),
        identity_pem: identity_from_string(s.get("identity_pem").unwrap()),
    })
}
