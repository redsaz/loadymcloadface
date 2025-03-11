use core::panic;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use reqwest::Method;

/// An entry in a URLs text file.
#[derive(Debug, Clone)]
pub struct UrlEntry {
    /// A URL part can either be a complete URL or just the end portion of a URL.
    pub urlpart: String,
    pub method: Method,
}

struct Tokenizer<'a> {
    text: &'a str,
    pos: usize,
}

/// Returns a Some(UrlEntry) if line had data in it, or None it was an empty line or a comment.
fn parse_line(line: &str) -> Option<UrlEntry> {
    // Some experimental results against Siege 4.1.7
    // - Variables can be specified in the url file with `NAME=value` on a single line
    // - Env vars can be referenced in the url file.
    // - At no point does siege's url parser handle quoting, so we don't need to do special
    //   parsing for that.
    // - There must be no space between the var name and the value (name=val, not name = val)
    // - It seems that variables cannot have pound (#), as that is ignored as part of a comment
    //   - Even when the variable comes from an env var, the pound and everything after is dropped.
    // - variable values specified in the url file do not have trailing spaces
    //   - But, if the value came from an env var, it can
    // - variable names can be any-caps hexadecimal or underscore
    // - When using ${} or $() for dereferencing variables, then it must not have a space inside
    // - multiple variables cannot be assigned in one line
    // - A variable is allowed to reference another variable
    // - A variable cannot dereference itself (whew). Example: RECURSE=SELF$(RECURSE)
    //   - it just looks like: SELF$(HEYHOWDY)
    // - A variable doesn't appear to be able to use array env var.
    // - If a variable is dereferenced that isn't assigned yet, then it is replaced with nothing:
    //   `http://$(NOVALUE).com` becomes `http://.com`
    // - If a variable is *later* assigned in the urls file, it gets used like normal.
    // - When the urls file is recycled, lines that dereferenced the variable before the variable
    //   was defined, will STILL NOT have a value.

    if line.is_empty() || line.chars().all(|c| c.is_whitespace()) {
        // Line is just whitespace or empty, it is not an entry.
        eprintln!("Skip whitespace");
        return None;
    } else if line.trim_start().starts_with("#") {
        // Line is a comment, it is not an entry.
        eprintln!("Skip comment");
        return None;
    }
    // There's better ways of doing this.
    // First item is the URL.
    let item = line.split_once(|c: char| c.is_whitespace());
    let mut entry = UrlEntry {
        urlpart: "".to_string(),
        method: Method::GET,
    };

    let mut next;
    if let Some((token, remaining)) = item {
        // There is a URL, and more tokens remain
        next = remaining;
        entry.urlpart = token.to_string();
    } else {
        // There is only one token, the URL, so we're done.
        entry.urlpart = line.to_string();
        return Some(entry);
    };

    // Next is the method, if any.
    let item = next.split_once(|c: char| c.is_whitespace());
    eprintln!("Well: {next}");
    if let Some((token, remaining)) = item {
        // There is a method, and more tokens remain
        next = remaining;
        let method = Method::from_bytes(token.as_bytes());
        if let Ok(method) = method {
            entry.method = method;
        } else {
            panic!("Not a valid HTTP Method: {}", token);
        }
        eprintln!("Woo! Method: {:?}", &entry.method);
    } else {
        // There wasn't anything after the method, so we're done.
        let method = Method::from_bytes(next.as_bytes());
        if let Ok(method) = method {
            entry.method = method;
        } else {
            panic!("Not a valid HTTP Method: {}", next);
        }
        return Some(entry);
    }

    // There is now a middle section of possibilities where multiple headers could be a thing.
    // Going to sleep on this for a while
    // loop {
    //     let next;
    //     let item = next.split_once(|c: char| c.is_whitespace());
    //     if let Some((token, remaining)) = item {
    //         next = remaining;
    //         eprintln!("Hey, we don't cover this yet: {}", token);
    //     } else {
    //         // Nothing else remained, so we're done.
    //         return Some(entry);
    //     }
    // }

    // At the end is the body.parse_line(&line.unwrap())

    Some(entry)
}

/// Load a list of URLs from a txt file, that (sometimes) conforms to Siege's URLs list.
///
/// Each line is an entry. Currently, all that is supported are URLs/paths.
///
/// Examples:
///
/// Full URLs can be used:
/// ```
/// https://www.example.com/path1/path2/foo
/// https://www.example.com/path1/path2/bar
/// https://www.example.com/path1/baz
/// ```
///
/// Or partial URLs with just the end of the path specified, and the beginning part of the URL,
/// called the "base URL", specified with `LOADY_BASEURL` environment variable. Assume
/// `LOADY_BASEURL=https://www.example.com/path1/` like in the first example:
/// ```
/// path2/foo
/// path2/bar
/// baz
/// ```
///
/// Or a mixture:
/// ```
/// path2/foo
/// https://www.example.com/path2/bar
/// https://www.example2.com/baz
/// ```
///
/// Note that starting slashes matter. If a base URL has a path component, and a URL from
/// the URLs file starts with a slash, then the base path portion is overwritten:
/// ```
/// # Example: LOADY_BASEURL=https://www.example.com/path1
/// path2/foo   # Becomes: https://www.example.com/path1/path2/foo
/// /path2/foo  # Becomes: https://www.example.com/path2/foo (See that /path1 portion is gone)
/// ```
///
/// Siege supports HTTP methods other than GET, and supports bodies:
/// ```
/// https://www.example.com/upload POST {"item1": "value1", "item2": [1, 2, 3]}
/// https://www.example.com/alter PUT {"item3": "value3", "item4": 4}
/// ```
///
/// Siege allows call-specific Content-Type header to be defined with `-T`:
/// ```
/// # The optional -T parameter requires ';' to conclude the Content-Type value in Siege:
/// https://www.example.com/upload POST -T application/json; {"example1":"value2"}
/// # This app does not require the ';' for the arg, it can be ignored:
/// https://www.example.com/upload POST -T application/json {"example1":"value2"}
/// # This app can also use quotes as needed for the arg:
/// https://www.example.com/upload POST -T "text/html; charset=utf-8" <html><body>Example</body></html>
/// ```
///
/// # Panics
///
/// This function panics if lines do not conform to UTF-8, or the file does not exist.
pub fn load(urls_txt: &Path) -> Vec<UrlEntry> {
    let mut entries = vec![];
    for line in BufReader::new(File::open(urls_txt).unwrap()).lines() {
        if let Some(entry) = parse_line(&line.unwrap()) {
            entries.push(entry);
        }
    }
    entries
}
