use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read, Result};
use std::ops::Sub;
use std::time::{Duration, Instant};

/// Number of cpu ticks per second.
static SC_CLK_TCK: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| ProcPidStats::sc_clk_tck());
/// When the process started.
static START_INSTANT: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(|| Instant::now());

pub fn init() {
    let _ = &*START_INSTANT;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Millicore(u64);

impl fmt::Display for Millicore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} millicore", self.0)
    }
}

impl Sub for Millicore {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Self(self.0 - other.0)
    }
}

pub trait CpuTime {
    fn user(&self) -> Millicore;
    fn system(&self) -> Millicore;
    fn elapsed(&self) -> Duration;
    fn at(&self) -> Instant;
}

pub trait CpuSpan {
    fn user(&self) -> Millicore;
    fn system(&self) -> Millicore;
    fn elapsed(&self) -> Duration;
    fn start(&self) -> impl CpuTime;
    fn end(&self) -> impl CpuTime;
}

/// Contains (some) information from the `/proc/$pid/stats` file.
#[derive(Clone, Copy, Hash, Debug)]
pub struct ProcPidStats {
    /// cpu user time
    user: Millicore,
    /// cpu kernel time
    system: Millicore,
    /// elapsed time, a.k.a. wall clock time, a.k.a. real time
    elapsed: Duration,
}

#[derive(Clone, Copy, Hash, Debug)]
pub struct CpuTimeLinux {
    stats: ProcPidStats,
    /// When the stats were captured
    pub captured_at: Instant,
}

#[derive(Debug)]
pub struct CpuSpanLinux {
    start: CpuTimeLinux,
    end: CpuTimeLinux,
}

impl ProcPidStats {
    fn sc_clk_tck() -> usize {
        // clock ticks is typically 100 Hz
        let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) as usize };
        tck
    }

    fn fetch() -> Result<ProcPidStats> {
        // Read /proc/$pid/stat to get
        // utime (ticks), stime (ticks), and start time (seconds since boot)
        let mut buf = [0u8; 8192];
        let size = BufReader::new(File::open("/proc/self/stat")?).read(&mut buf)?;

        // Find the last index where ')' is found, and read from after the next byte to the end,
        // in order to skip the filename (which could be non-utf8, or have one or more
        // parentheses)
        let mut post_filename_pos = 0;

        for (i, char) in buf.iter().enumerate() {
            if *char == 0x29u8 {
                post_filename_pos = i + 2;
            }
        }
        // The part *after* the filename is safe to convert to utf8 string.
        let mut utime = Option::None;
        let mut stime = Option::None;
        // let mut starttime = Option::None;
        let values = std::str::from_utf8(&buf[post_filename_pos..size]).unwrap();
        for (i, val) in values.split(' ').enumerate() {
            if i == 11 {
                utime = Option::Some(val.parse::<u64>().unwrap());
            } else if i == 12 {
                stime = Option::Some(val.parse::<u64>().unwrap());
                // } else if i == 19 {
                //     starttime = Option::Some(val.parse::<u64>().unwrap());
            }
        }

        // // Step 2: read /proc/uptime to get uptime (in seconds, including in suspend)
        // let size = BufReader::new(File::open("/proc/uptime")?).read(&mut buf)?;

        // TODO: ACTUALLY MAKE THIS
        let sc_clk_tck = *SC_CLK_TCK;
        Result::Ok(ProcPidStats {
            system: Millicore(stime.unwrap() * 1000 / sc_clk_tck as u64),
            user: Millicore(utime.unwrap() * 1000 / sc_clk_tck as u64),
            elapsed: (*START_INSTANT).elapsed(),
        })
    }
}

impl CpuSpan for CpuSpanLinux {
    fn user(&self) -> Millicore {
        self.end.user() - self.start.user()
    }

    fn system(&self) -> Millicore {
        self.end.system() - self.start.system()
    }

    fn elapsed(&self) -> Duration {
        self.end.elapsed() - self.start.elapsed()
    }

    fn start(&self) -> impl CpuTime {
        self.start
    }

    fn end(&self) -> impl CpuTime {
        self.end
    }
}

impl CpuTimeLinux {
    /// Capture new cpu time. Since this is the first capture, wall clock time is calculated
    /// by also finding out the system's current time and subtracting the process's start time.
    pub fn init() -> CpuTimeLinux {
        let captured_at = Instant::now();
        let stats = ProcPidStats::fetch().expect("Unable to get CPU usage.");

        CpuTimeLinux { stats, captured_at }
    }

    /// Capture new cpu time, differenced this cpu time.
    ///
    /// # Example:
    /// ```
    /// let start = CpuStats::init();
    /// // Do stuff
    /// let part1 = start.since();
    /// eprintln!("user cpumillis during part1: {}", part1.user_cpumillis());
    /// // Do more stuff
    /// let part2 = part1.since();
    /// eprintln!("user cpumillis during part2: {}", part2.user_cpumillis());
    /// let overall = start.since(); // Get the stats since the start again, to get overall time.
    /// eprintln!("overall user cpumillis: {}", overall.user_cpumillis());
    /// ```
    pub fn since(self: &CpuTimeLinux) -> CpuSpanLinux {
        CpuSpanLinux {
            start: self.clone(),
            end: CpuTimeLinux::init(),
        }
    }
}

impl CpuTime for CpuTimeLinux {
    fn at(&self) -> Instant {
        self.captured_at
    }
    fn system(&self) -> Millicore {
        self.stats.system
    }
    fn user(&self) -> Millicore {
        self.stats.user
    }
    fn elapsed(&self) -> Duration {
        self.stats.elapsed
    }
}

pub fn cpu() -> CpuTimeLinux {
    CpuTimeLinux::init()
}
