use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

/// This originally (mostly) adhered to the HAR spec:
/// http://www.softwareishard.com/blog/har-12-spec/
/// but much of it is repetitive and bloat if the intent is only to capture
/// enough of the request and response in order to do, say, post-test cleanup.
/// Parts of the HAR spec will be trimmed away as needed, hence the name,
/// Trimmed HTTP Archive (THAR). When each one is a single line in a file,
/// it becomes THARL (like JSONL).
///
/// Note: THAR will not serialize null values or empty arrays.

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DISPLAY_NAME: &str = "Loady McLoadface";

/// Required in HAR, not used in THAR.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Creator {
    pub name: String,
    pub version: String,
    //pub comment: Option<String>,
}

/// Required in HAR, not used in THAR.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Browser {
    pub name: String,
    pub version: String,
    //pub comment: Option<String>,
}

/// Required in HAR, not used in THAR. It's more browser-centric, but loady is
/// more HTTP API centric.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageTimings {
    /// When the core page itself loads.
    pub on_content_load: i64,
    /// When the images, scripts, and whatnot have all completed. Since
    /// Loady does not parse any of the content to figure out what else
    /// needs loaded, this will be the same as on_content_load.
    pub on_load: i64,
    //pub comment: Option<String>,
}

/// Required in HAR, optional in THAR.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    #[serde(serialize_with = "to_rfc3339_millis")]
    pub started_date_time: DateTime<Utc>,
    pub id: String,
    pub title: String,
    pub page_timings: PageTimings,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cookie {
    pub name: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secure: Option<bool>,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub name: String,
    pub value: String,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostDataParam {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostData {
    pub mime_type: String,
    pub text: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<PostDataParam>,
    //pub comment: Option<String>,
}

/// THAR does not use the following:
/// - queryString: the part is already in url
/// - httpVersion: ALL requests in a run will be the same http version, no need
/// - headersSize: This can be found out from looking at the headers array
/// - bodySize: This can be found out from looking at postData
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub method: String,
    pub url: String,
    //pub http_version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cookies: Vec<Cookie>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<Record>,
    //pub query_string: Vec<Record>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_data: Option<PostData>,
    //pub headers_size: i64,
    //pub body_size: i64,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<i64>,
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    //pub comment: Option<String>,
}

/// THAR does not use the following:
/// - statusText: This *can* be useful, but usually only status is.
/// - httpVersion: Loady only uses a single HTTP version per run.
/// - redirectURL: This *can* be useful, but usually not.
/// - headersSize: This can be found out from looking at the headers array
/// - bodySize: This can be found out from looking at postData
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub status: i32,
    //pub status_text: String,
    //pub http_version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cookies: Vec<Cookie>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<Record>,
    pub content: Content,
    //pub redirect_u_r_l: String,
    //pub headers_size: i64,
    //pub body_size: i64,
    //pub comment: Option<String>,
}

/// Not used by THAR, it is browser centric.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<String>,
    #[serde(serialize_with = "to_rfc3339_millis")]
    pub last_access: DateTime<Utc>,
    pub e_tag: String,
    pub hit_count: i64,
    //pub comment: Option<String>,
}

/// Required by HAR, but not used by THAR since Loady doesn't cache anything.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cache {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_request: Option<CacheEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_request: Option<CacheEntry>,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Timings {
    pub dns: i64,
    pub connect: i64,
    pub blocked: i64,
    pub send: i64,
    pub wait: i64,
    pub receive: i64,
    /// If defined, this is part of the connection timing.
    /// See: http://www.softwareishard.com/blog/har-12-spec/#timings
    pub ssl: i64,
    //pub comment: Option<String>,
}

impl Timings {
    /// Adds up all timings and returns the total.
    /// (ssl timing is not included, it is part of connect timing.)
    pub fn total(&self) -> i64 {
        return if self.dns > 0 { self.dns } else { 0_i64 }
            + if self.connect > 0 {
                self.connect
            } else {
                0_i64
            }
            + if self.blocked > 0 {
                self.blocked
            } else {
                0_i64
            }
            + if self.send > 0 { self.send } else { 0_i64 }
            + if self.wait > 0 { self.wait } else { 0_i64 }
            + if self.receive > 0 {
                self.receive
            } else {
                0_i64
            };
    }
}

/// THAR does not use the following that are required in HAR:
/// - cache: Loady does not cache results.
/// - timings: This *is* useful, and may be included in a later version.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pageref: Option<String>,
    #[serde(serialize_with = "to_rfc3339_millis")]
    pub started_date_time: DateTime<Utc>,
    /// Total duration from start of request to last byte received, in millis.
    pub time: i64,
    pub request: Request,
    pub response: Response,
    //pub cache: Cache,
    //pub timings: Timings,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_i_p_address: Option<String>,
    /// An identifier to distinguish one connection from another. That is,
    /// requests that are sent in the same TCP connection would have one id,
    /// and requests sent in a different TCP connection would have another,
    /// and so on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    //pub comment: Option<String>,
}

/// Used as the root node of THAR.
impl Entry {
    pub fn new(
        started_date_time: DateTime<Utc>,
        request: Request,
        response: Response,
        total_time: i64,
    ) -> Entry {
        Entry {
            pageref: None,
            started_date_time,
            // Since this is a single request, this matches page timings.
            time: total_time,
            request,
            response,
            server_i_p_address: None,
            connection: None,
        }
    }
}

/// Required by HAR, not used by THAR. Loady treats each request as a single
/// page, and most of the items of a Log are repetitive between each entry,
/// so should not be used by THAR.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub version: String,
    pub creator: Creator,
    pub browser: Browser,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pages: Vec<Page>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<Entry>,
    //pub comment: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Har {
    pub log: Log,
}

impl Har {
    /// Create a new HAR-log for a single request.
    pub fn new(
        started_date_time: DateTime<Utc>,
        request: Request,
        response: Response,
        timings: Timings,
    ) -> Har {
        let total_time = timings.total();
        let log = Log {
            version: "1.2".to_string(),
            creator: Creator {
                name: DISPLAY_NAME.to_string(),
                version: VERSION.to_string(),
            },
            browser: Browser {
                name: DISPLAY_NAME.to_string(),
                version: VERSION.to_string(),
            },
            pages: vec![Page {
                started_date_time,
                id: "page_1".to_string(),
                title: "sample".to_string(),
                // Since this is a single request, both page timings are the
                // same, *and* match entries.time.
                page_timings: PageTimings {
                    on_content_load: total_time,
                    on_load: total_time,
                },
            }],
            entries: vec![Entry {
                pageref: Some("page_1".to_string()),
                started_date_time,
                // Since this is a single request, this matches page timings.
                time: total_time,
                request,
                response,
                // Commented out because THAR will be used, not HAR.
                // cache: Cache {
                //     before_request: None,
                //     after_request: None,
                // },
                // timings,
                server_i_p_address: None,
                connection: None,
            }],
        };

        Har { log }
    }
}

/// https://serde.rs/field-attrs.html#serialize_with
fn to_rfc3339_millis<S>(datetime: &DateTime<Utc>, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let str = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    ser.serialize_str(&str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::to_string;

    #[test]
    fn test_log() {
        let log = Log {
            version: "1.2".to_string(),
            creator: Creator {
                name: DISPLAY_NAME.to_string(),
                version: VERSION.to_string(),
            },
            browser: Browser {
                name: DISPLAY_NAME.to_string(),
                version: VERSION.to_string(),
            },
            pages: vec![Page {
                started_date_time: Utc.with_ymd_and_hms(2026, 3, 16, 13, 14, 15).unwrap(),
                id: "page_1".to_string(),
                title: "sample".to_string(),
                page_timings: PageTimings {
                    on_content_load: 100,
                    on_load: 100,
                },
            }],
            entries: vec![Entry {
                pageref: Some("page_1".to_string()),
                started_date_time: Utc.with_ymd_and_hms(2026, 3, 16, 13, 14, 15).unwrap(),
                time: 100,
                request: Request {
                    method: "GET".to_string(),
                    url: "http://127.0.0.1:8080/logs/test".to_string(),
                    http_version: "HTTP/1.1".to_string(),
                    cookies: vec![],
                    headers: vec![Record {
                        name: "Accept".to_string(),
                        value: "application/json".to_string(),
                    }],
                    query_string: vec![],
                    post_data: None,
                    headers_size: -1,
                    body_size: 0,
                },
                response: Response {
                    status: 200,
                    status_text: "OK".to_string(),
                    http_version: "HTTP/1.1".to_string(),
                    cookies: vec![],
                    headers: vec![],
                    content: Content {
                        size: 5,
                        compression: None,
                        mime_type: "text/html; charset=utf-8".to_string(),
                        text: None,
                        encoding: None,
                    },
                    redirect_u_r_l: "".to_string(),
                    headers_size: 0,
                    body_size: 5,
                },
                cache: Cache {
                    before_request: None,
                    after_request: None,
                },
                timings: Timings {
                    dns: -1,
                    connect: -1,
                    blocked: -1,
                    send: 1,
                    wait: 10,
                    receive: 90,
                    ssl: -1,
                },
                server_i_p_address: Some("127.0.0.1".to_string()),
                connection: None,
            }],
        };
        let har = Har { log };
        println!("{}", to_string(&har).expect("Failed to serialize."));
        // assert_eq!();
    }
}
