use std::fs::File;
use std::io::{BufReader, Read, Result};
use std::ops::Sub;
use std::time::{Duration, Instant};

/// Number of cpu ticks per second.
static SC_CLK_TCK: std::sync::LazyLock<usize> = std::sync::LazyLock::new(ProcPidStats::sc_clk_tck);
/// When the process started.
static START_INSTANT: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

pub fn init() {
    let _ = &*START_INSTANT;
}

/// Contains (some) information from the `/proc/$pid/stats` file.
#[derive(Clone, Copy, Hash, Debug)]
pub struct ProcPidStats {
    /// elapsed time, a.k.a. wall clock time, a.k.a. real time.
    pub elapsed: Duration,
    /// Amount of cpu user time consumed during the elapsed time.
    /// Multicore can potentially make this greater than elapsed.
    pub user: Duration,
    /// Amount of cpu kernel time consumed during the elapsed time.
    /// Multicore can potentially make this greater than elapsed.
    pub system: Duration,
}
impl Sub for ProcPidStats {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Self {
            elapsed: self.elapsed - other.elapsed,
            user: self.user - other.user,
            system: self.system - other.system,
        }
    }
}
impl ProcPidStats {
    /// Capture new cpu time. Since this is the first capture, wall clock time is calculated
    /// by also finding out the system's current time and subtracting the process's start time.
    pub fn cpu() -> ProcPidStats {
        ProcPidStats::fetch().expect("Unable to get CPU usage.")
    }

    /// Returns the amount of cores used.
    /// This is the system and user times divided by the elapsed time.
    pub fn cpu_cores(&self) -> f32 {
        (self.system + self.user).div_duration_f32(self.elapsed)
    }

    fn sc_clk_tck() -> usize {
        // clock ticks is typically 100 Hz
        unsafe { libc::sysconf(libc::_SC_CLK_TCK) as usize }
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

        let sc_clk_tck = *SC_CLK_TCK;
        Result::Ok(ProcPidStats {
            elapsed: (*START_INSTANT).elapsed(),
            system: Duration::from_millis(stime.unwrap() * 1000 / sc_clk_tck as u64),
            user: Duration::from_millis(utime.unwrap() * 1000 / sc_clk_tck as u64),
        })
    }
}

/// Same as calling ProcPidStats::cpu()
pub fn cpu() -> ProcPidStats {
    ProcPidStats::cpu()
}

pub fn duration_hms(d: Duration) -> (u32, u8, u8) {
    let sec = d.as_secs();
    let min = sec / 60 % 60;
    let hour = sec / 3600;
    let sec = sec % 60;
    (hour as u32, min as u8, sec as u8)
}
