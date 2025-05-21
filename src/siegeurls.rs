use core::panic;
use crossbeam::channel::{bounded, Receiver};
use reqwest::Method;
use std::env;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Lines},
    iter::Iterator,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone)]
pub enum BodyData {
    /// No body with the request.
    None,
    /// The body, loaded into memory.
    Content(Vec<u8>),
    /// The file containing the body content.
    File(PathBuf),
}

/// The method and URL to make a call with.
#[derive(Debug, Clone)]
pub struct UrlEntry {
    pub delay: Duration,
    /// A URL part can either be a complete URL or just the end portion of a URL.
    pub urlpart: String,
    pub method: Method,
    pub content_type: Option<String>,
    pub body: BodyData,
}

/// Allows fetching a stream of UrlEntry items.
#[derive(Debug)]
pub struct SiegeUrls {
    variables: HashMap<String, String>,
    default_delay: Duration,
    lines: Lines<BufReader<File>>,
}

impl Iterator for SiegeUrls {
    type Item = UrlEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = self.lines.next();
            if let Some(l) = line {
                let line = l.unwrap();
                let entry = self.parse_line(line.as_str(), self.default_delay.clone());
                if entry.is_some() {
                    return entry;
                }
            } else {
                return None;
            }
        }
    }
}

#[derive(Debug)]
enum Mode {
    /// Not in an escape, not in a reference, just regular text
    Normal,
    /// Previous char was the escape '\' char
    Escape,
    /// Previous char was the reference '$' char
    RefStart,
    /// Getting the variable name of the reference, not wrapped by brace or parens
    RefNoWrap,
    /// Getting the variable name of the reference, wrapped with braces '{' '}'
    RefWithBrace,
    /// Getting the variable name of the reference, wrapped with parens '(' ')'
    RefWithParen,
}

impl SiegeUrls {
    fn parse_assignment<'a>(line: &'a str) -> Option<(&'a str, &'a str)> {
        let line = line.trim();
        let assign = line.split_once("=");

        // If "=" doesn't appear then this is not an assignment line.
        if assign.is_none() {
            return None;
        }
        // If any non-alphanumeric characters or underscore appear before the "=" (save for
        // whitespace), then this is not an assignment line.
        // For example, any of ":", "?", "/", "$" appearing before "=" would be expected for legit
        // URLs.
        let (name, value) = assign.unwrap();
        if name.contains(|c: char| !c.is_alphanumeric() && c != '_') {
            return None;
        }

        // If comment char (#) appears in value side, then remove it and trailing whitespace.
        if let Some((value, _comment)) = value.split_once('#') {
            return Some((name, value.trim_end()));
        }

        assign
    }

    /// Looks up the value of variable `var_name` and inserts it at the end of updated String.
    /// If the variable is not found, then the name will be looked up as an environment variable
    /// and inserted. If the env var is not found, then the default_val is inserted.
    fn insert_val(self: &SiegeUrls, var_name: &str, updated: &mut String, default_val: &str) {
        if let Some(s) = self.variables.get(var_name) {
            eprintln!("Found var. Name: {} value: {}", var_name, s);
            updated.push_str(s);
        } else if let Some(s) = env::var_os(var_name) {
            let val = s.to_string_lossy();
            eprintln!("Found env var. Name: {} value: {}", var_name, val);
            updated.push_str(&val);
        } else {
            eprintln!(
                "Did not find var or env var. Name: {} default: {}",
                var_name, default_val
            );
            updated.push_str(default_val);
        };
    }

    /// Given a line of text, if any `$(VAR_NAME)` is found, text is substituted
    /// and a new String is returned.
    fn replace_vars(self: &SiegeUrls, line: &str) -> String {
        // Only do all the fancy escaping and substitution if we have to.
        if let Some(i) = line.find(['\\', '$']) {
            let mut updated = String::with_capacity(line.len() + 16);
            if i > 0 {
                updated.push_str(&line[..i]);
            }
            let mut mode = Mode::Normal;
            let mut var_start = 0; // will be updated later
            let mut var_name = &line[0..1]; // will be updated later
            let line = &line[i..];
            for (i, c) in line.chars().enumerate() {
                match mode {
                    Mode::Normal => {
                        // Yup this gets copied and pasted later
                        if c == '\\' {
                            mode = Mode::Escape
                        } else if c == '$' {
                            mode = Mode::RefStart
                        } else if c == '#' {
                            // A comment char in normal mode ignores the rest of the line.
                            break;
                        } else {
                            updated.push(c)
                        }
                    }
                    Mode::Escape => {
                        // Far as I can tell:
                        // - Siege only uses '\' to escape '$', but I'm making it escape '\' too.
                        // - If Siege sees any other character, then the escaping "cancels", and so
                        //   the '\' *and* the char after it are inserted.
                        if c != '$' && c != '\\' {
                            updated.push('\\');
                        }
                        updated.push(c);
                        mode = Mode::Normal
                    }
                    Mode::RefStart => {
                        // Previous char was '$'. If this char is a...
                        mode = match c {
                            // ...alphanumeric-or-underscore char, build var name, end at
                            // non-alphanumeric-or-underscore char
                            '0'..='9' | 'a'..='z' | 'A'..='Z' => {
                                var_start = i;
                                Mode::RefNoWrap
                            }
                            // ...brace, then build the var name, end at close brace
                            '{' => {
                                var_start = i + 1; // the start is actually the next char
                                Mode::RefWithBrace
                            }
                            // ...paren, then build the var name, end at close paren
                            '(' => {
                                var_start = i + 1; // the start is actually the next char
                                Mode::RefWithParen
                            }
                            // ...anything else, then it was a literal '$', which means that
                            // we should actually be in "Normal" mode now.
                            _ => {
                                updated.push('$');
                                // Yup this is copy-pasted from above Normal mode
                                if c == '\\' {
                                    Mode::Escape
                                } else if c == '$' {
                                    Mode::RefStart
                                } else {
                                    updated.push(c);
                                    Mode::Normal
                                }
                            }
                        }
                    }
                    Mode::RefNoWrap => {
                        // A non-alphanumeric-or-underscore char means we've reached the end of the var name
                        if !c.is_alphanumeric() && c != '_' {
                            var_name = &line[var_start..i];
                            self.insert_val(var_name, &mut updated, "");
                            // Must insert the non-alphanumeric non-underscore char too
                            updated.push(c);

                            mode = Mode::Normal;
                        }
                    }
                    Mode::RefWithBrace => {
                        // A close brace char means we've reached the end of the var name
                        if c == '}' {
                            if var_start != i {
                                var_name = &line[var_start..i];
                                self.insert_val(var_name, &mut updated, "");
                            } else {
                                // if var_start and i are the same, then "${}" are the latest chars
                                // TODO: find out how siege handles this. We're going to act as if
                                // these are literal characters, since that seems in the spirit of
                                // siege.
                                updated.push_str("${}");
                            }
                            mode = Mode::Normal
                        }
                    }
                    Mode::RefWithParen => {
                        // A close paren char means we've reached the end of the var name
                        if c == ')' {
                            if var_start != i {
                                var_name = &line[var_start..i];
                                self.insert_val(var_name, &mut updated, "");
                            } else {
                                // if var_start and i are the same, then "$()" are the latest chars
                                // TODO: find out how siege handles this. We're going to act as if
                                // these are literal characters, since that seems in the spirit of
                                // siege.
                                updated.push_str("$()");
                            }
                            mode = Mode::Normal;
                        }
                    }
                }
            }
            // If the loop is left while in a non-normal mode, it means that something is
            // incomplete. The user ended...
            match mode {
                Mode::Normal => { /* skip */ }
                // ...on an escape '\', so it's treated like a literal.
                Mode::Escape => {
                    updated.push('\\');
                }
                // ...on a ref start '$', so it's treated like a literal.
                // (Note: siege would segfault on this)
                Mode::RefStart => {
                    updated.push('$');
                }
                // ...with a completed no-wrap reference, so look up its var and insert it
                Mode::RefNoWrap => {
                    var_name = &line[var_start..];
                    self.insert_val(var_name, &mut updated, "");
                }
                // ...with an incompleted un-closed reference, so treat the whole ref as a literal
                Mode::RefWithBrace | Mode::RefWithParen => {
                    // A close brace char means we've reached the end of the var name
                    updated.push_str(&line[(var_start - 2)..]);
                }
            }
            return updated;
        } else {
            return line.to_owned();
        }
    }

    /// Given the remaining of a line of text, determines if there is a Content-Type part and a
    /// body part.
    /// (If the line starts with "-T" then there is a Content-Type part, otherwise all of it is
    /// the body.)
    fn get_type_and_body<'a>(
        self: &SiegeUrls,
        line: &'a str,
    ) -> (Option<&'a str>, Option<&'a str>) {
        // If starts with -T, then its a Content-Type part
        if line.starts_with("-T") {
            if let Some((header, body)) = line[2..].split_once(';') {
                (Some(header.trim()), Some(body.trim()))
            } else {
                // It turns out there was no closing semi-colon that siege 4.1.7 uses to close
                // the Content-Type. (and why semi-colon? That's a legit char in that header.)
                // Not sure what the best thing to do here is, so we'll say that the entire
                // rest of the line is the header. Sure.
                (Some(&line[2..].trim()), None)
            }
        } else {
            (None, Some(line.trim()))
        }
    }

    /// Given the remaining of a line of text, determines if there is a Content-Type part and a
    /// body part.
    /// (If the line starts with "-T" then there is a Content-Type part, otherwise all of it is
    /// the body.)
    fn load_body_if_redirected(self: &SiegeUrls, body_opt: Option<&str>) -> BodyData {
        if let Some(body) = body_opt {
            if body.starts_with('<') {
                // We were not given a body, but a filename to load the body from
                let filename = &body[1..].trim();
                let file = Path::new(filename);
                if file.is_file() {
                    // If the file is under a certain size, load it up now, otherwise get the
                    // filename so it can be loaded later.
                    if std::fs::metadata(file).map_or(false, |meta| meta.len() <= 16384) {
                        let content = std::fs::read(file).unwrap_or_default();
                        BodyData::Content(content)
                    } else {
                        BodyData::File(file.to_path_buf())
                    }
                } else {
                    // There was no file, so do not provide a body.
                    BodyData::None
                }
            } else {
                // there is no redirect, so the given string *is* the body.
                BodyData::Content(body.as_bytes().to_vec())
            }
        } else {
            BodyData::None
        }
    }

    /// Returns a Some(UrlEntry) if line had data in it, or None it was an empty line or a comment.
    fn parse_line(self: &mut SiegeUrls, line: &str, default_delay: Duration) -> Option<UrlEntry> {
        // Some experimental results against Siege 4.1.7
        // - Variables can be specified in the url file with `NAME=value` on a single line
        // - Env vars can be referenced in the url file.
        // - Underscore '_' is acceptable in a var name, but not a dash '-'
        // - At no point does siege's url parser handle quoting, so we don't need to do special
        //   parsing for that.
        // - Only at the very end of a line does a dereference without parens or braces actually
        //   work. However
        // - There must be no space between the var name and the value (name=val, not name = val)
        // - The pound (#) at any point in a URL entry or assignment treats it as a comment from
        //   that point on. This is fine, since this component of a URL is not passed to the server
        //   ever.
        // - It seems that variables cannot have pound (#), as that is ignored as part of a comment
        //   - Even when the variable comes from an env var, the pound and everything after is
        //     dropped.
        // - variable values specified in the url file do not have trailing spaces
        //   - But, if the value came from an env var, it can
        // - variable names can be any-caps hexadecimal or underscore
        // - When using ${} or $() for dereferencing variables, then it must not have a space inside
        // - multiple variables cannot be assigned in one line
        // - A variable is allowed to reference another variable
        // - A variable cannot dereference itself (whew). Example: RECURSE=SELF$(RECURSE)
        //   - it just looks like: SELF$(RECURSE)
        // - A variable doesn't appear to be able to use array env var.
        // - When a value is dereferenced before the equals, it is treated as a url, not assignment.
        //   ```
        //   INDEX=2
        //   ITEM$(INDEX)=Test
        //   # The above, one might expect it to be equivalent to `ITEM2=Test`, but no.
        //   ```
        // - If a variable is dereferenced that isn't assigned yet, then it is replaced with
        //   nothing: `http://$(NOVALUE).com` becomes `http://.com`
        // - If a variable is *later* assigned in the urls file, it gets used like normal.
        // - When the urls file is recycled, lines that dereferenced the variable before the
        //   variable was defined, will STILL NOT have a value.
        // - Escaping is weird? Haven't looked at the docs beyond "\$" escapes to "$" without
        //   needing to worry about substitution.
        //   - BUT, "\\" DOESN'T escape to "\", which means that if you want to represent the
        //     literal string "\$"... you can't?
        //   - In fact, putting "\\" anywhere before a $ (even immediately before) will disable
        //     substitution.
        //   - Putting "\\" anywhere after the last $ will insert two backslashes.
        //   - Whether this "\\" behavior is intentional or not, I don't know, but it seems not
        //     useful.
        //   - So we'll do this instead:
        //     - "\\" escapes to "\".
        //     - "\$" escapes to "$" without worrying about variable substitution.
        //     - So, if "\\$ABC" appears: a literal "\" appears, followed by whatever the ABC
        //       attribute completes to, beccause the "$" wasn't escaped by a backslash.
        // - When a VAR has spaces in it, it can cover any or ALL the parts of a url entry.
        //     - Example:
        //       EXAMPLE=example_value PUT -T application/json;
        //       http://127.0.0.1:8080/reviews?example=${EXAMPLE}
        //     - That example acts as if I typed out this line:
        //       http://127.0.0.1:8080/reviews?example=example_value PUT -T application/json;
        //     - Which means yes, a VAR can be used for multiple segments of a url entry.
        //     - This is *also* the case for the body part, *even* when it is a redirect:
        //       EXAMPLE=example_value PUT <redirect/to/file
        //     - This seems to indicate that var substituion happens after its been established
        //       if the line is an assignment line or url entry.
        //       - This is because having this...
        //         PART1=PART2=2
        //         ${PART1}
        //         ...won't cause PART2 to become a var.
        // - What about "${UNSET_VARIABLE}" appears?
        //   - It is replaced by blank (that is, "". Nothing.)
        // - What happens if "${FOO}" is content in a redirected file?
        //   - Answer: Nothing. No substitutions happen there.
        // - What happens if "${..." (with no closing "}") is encountered?
        //   - Answer: the rest of the line is treated as the var name.
        //     - If there is no mapping, then it is replaced by blank, like usual.
        //     - If there is a mapping, it is replaced by the value, like usual.
        // - What about "${}" (nothing inside)?
        //     - It results in a segmentation fault.
        // - What happens if a comment appears in a var reference?
        //   TEST=Fred
        //   http://127.0.0.1:8080/${TEST#Comment}/Bob
        //   ...The above will turn into this: http://127.0.0.1:8080//Bob
        //   ...If the comment was processed before the substitution, then
        //   the line would turn into this: http://127.0.0.1:8080/Fred
        //   but it does not, so therefore the comment processing happens later after
        //   variable assignment.
        //
        // - Here's a normal line that specifies a URL, method, content-type, and body that
        //   works as expected:
        //   http://127.0.0.1:8080 POST -T application/json; {"key": "value"}
        // - What happens when...
        //   - ...No content-type header is specified?
        //     http://127.0.0.1:8080 POST {"key": "value"}
        //     - Then is fine; the content-type header doesn't show up is all.
        //   - ...No body is specified?
        //     http://127.0.0.1:8080 POST
        //     - This is fine; there s a content-length of 0.
        //   - ...No method is specified, but body is?
        //     http://127.0.0.1:8080 {"key": "value"}
        //     - This is NOT fine; it is treated as a GET, and this url is requested:
        //     http://127.0.0.1:8080%20%7B%22k%22:%20%22v4%22%7D
        //     - It would've been better if it defaulted to POST when a body is specified.
        //     - In fact, it is possible for several space-separated tokens to be between the
        //       URL and the method, and those tokens are treated as part of the URL:
        //       http://127.0.0.1:8080 this has some more tokens POST {"key": "value"}
        //       - Siege will find the POST method, and everything up to that point is treated as
        //         part of the URL.
        //   - ...There is a comment in the line?
        //     http://127.0.0.1:8080# This is an example comment
        //     - Answer: Everything up to the comment is treated as part of the URL, including
        //       any spaces between the url and the comment char. Yes, really.
        //   - ...There is a method and a comment?
        //     http://127.0.0.1:8080 PUT# Comment
        //     - Answer: The pound and everything past it is considered part of the body.
        //       - A space between the method and the pound doesn't matter.
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

        // Either the line is a URL entry, or an assignment...
        // ...if the line is an assignment...
        if let Some((name, value)) = SiegeUrls::parse_assignment(line) {
            eprintln!(
                "Found an assignment.\n\tline: {}\n\t name: {}\n\tvalue: {}",
                line, name, value
            );
            // Only set the variable if it was never set before (according to experimenting with
            // the siege url file handler.)
            if !self.variables.contains_key(name) {
                let value = self.replace_vars(value);
                eprintln!("Adding variable. Name: {} Value: {}", name, value);
                self.variables.insert(name.to_owned(), value);
            };
            return None;
        }

        // ...otherwise, the line is a URL entry, and the first item is the URL
        eprintln!("Found a url entry. line: {}", line);
        // replacement of vars happens before the parts of the url entry are defined so that the
        // parts could be defined by the var value, if need be.
        let line = self.replace_vars(line);

        let item = line.split_once(|c: char| c.is_whitespace());
        let mut entry = UrlEntry {
            delay: default_delay,
            urlpart: "".to_string(),
            method: Method::GET,
            content_type: None,
            body: BodyData::None,
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

        // Next MAY be the the method. If the token is not a recognized http method like
        // GET, PUT, POST, etc) then the token is as a part of the url.
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

        // Siege has a special case for Content-Type header (and *only* this header) just before
        // the body part.
        let (content_type_opt, body_opt) = self.get_type_and_body(next);
        if let Some(content_type) = content_type_opt {
            entry.content_type = Some(content_type.to_owned());
        }
        entry.body = self.load_body_if_redirected(body_opt);

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
    pub fn load(urls_txt: &Path, default_delay: Duration) -> Vec<UrlEntry> {
        let siege_urls = SiegeUrls {
            variables: HashMap::new(),
            default_delay,
            lines: BufReader::new(File::open(urls_txt).unwrap()).lines(),
        };
        siege_urls.collect()
    }

    /// Iterates over a urls file until it completes.
    pub fn load_iter(urls_txt: &Path, default_delay: Duration) -> SiegeUrls {
        SiegeUrls {
            variables: HashMap::new(),
            default_delay,
            lines: BufReader::new(File::open(urls_txt).unwrap()).lines(),
        }
    }

    /// Iterates over a urls file and loops to the beginning when the end is reached.
    ///
    /// * stride - how many url entries to skip, minimum value must be 1
    /// * offset - which url entry within the stride to skip, range is 0 to stride - 1
    ///
    /// stride and offset allow a url list to be split across different instances of the app
    /// running concurrently. For example, if running 4 instances, the stride should be `4`,
    /// and the offset should be `0`, `1`, `2`, `3` in the first, second, third, and fourth
    /// instances, respectively.
    ///
    /// Note that the delay between each call is adjusted to act as if the skipped calls *were*
    /// called. In other words, if the stride is 4, then the delay between each call is multiplied
    /// by 4. Additionally, if the offset is non-zero, like say, 1, then the appropriate delay
    /// will be added to the first call.
    pub fn load_iter_looping_buffered(
        urls_txt: &Path,
        default_delay: Duration,
        stride: usize,
        offset: usize,
    ) -> Receiver<UrlEntry> {
        // TODO: Rather than doing all this, why not pass in a callback instead, which can return a
        // Result<(), DoneSignal> that this function can check if things are done or not?
        let (tx, rx) = bounded(1000);

        let urls_txt = urls_txt.to_path_buf();
        let default_delay = default_delay.clone();
        let mut current_offset: usize = 0;
        let mut added_delay = Duration::ZERO;

        std::thread::spawn(move || {
            let mut urls = SiegeUrls::load_iter(urls_txt.as_path(), default_delay);
            loop {
                let mut url_entry = urls.next();
                if url_entry.is_none() {
                    // eprintln!("End of urls file. Done.");
                    // break;
                    // TODO! THIS IS TEMPORARY. Delete the above eprint and break, and uncomment
                    // the below to allow the function to recycle the file.
                    urls = SiegeUrls::load_iter(urls_txt.as_path(), default_delay);
                    url_entry = urls.next();
                    if url_entry.is_none() {
                        eprintln!("No URLs could be read from file {:?}, leaving.", urls_txt);
                        break;
                    }
                }
                let mut url_entry = url_entry.unwrap();
                // Only queue the url entry if we're in the right position to run it, according to
                // the offset and stride passed in.
                if current_offset == offset {
                    let delay = url_entry.delay.saturating_add(added_delay);
                    url_entry.delay = delay;
                    added_delay = Duration::ZERO;
                    eprintln!("Entry: {:?}", url_entry);

                    if let Err(_) = tx.send(url_entry) {
                        eprintln!("Channel disconnected, will not send any more URLs.");
                        break;
                    }
                } else {
                    added_delay = added_delay.saturating_add(url_entry.delay);
                }

                current_offset = (current_offset + 1) % stride;
            }
        });

        rx
    }
}
