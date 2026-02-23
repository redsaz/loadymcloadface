use std::fs::File;
use std::io::{BufReader, Error, Read, Result};
use std::ops::Sub;
use std::time::{Duration, Instant};

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

    fn fetch() -> Result<ProcPidStats> {
        unsafe {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) == 0 {
                let usage = usage.assume_init();
                let system_time = Duration::new(
                    usage.ru_stime.tv_sec as u64,
                    (usage.ru_stime.tv_usec * 1_000) as u32,
                );
                let user_time = Duration::new(
                    usage.ru_utime.tv_sec as u64,
                    (usage.ru_utime.tv_usec * 1_000) as u32,
                );
                Result::Ok(ProcPidStats {
                    elapsed: (*START_INSTANT).elapsed(),
                    system: system_time,
                    user: user_time,
                })
            } else {
                Result::Err(Error::other("Could not fetch usage info"))
            }
        }
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
