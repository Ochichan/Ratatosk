use std::{io, mem::MaybeUninit};

#[derive(Debug, Clone, Copy, Default)]
pub struct Usage {
    pub user_seconds: f64,
    pub system_seconds: f64,
    pub max_rss_bytes: u64,
}

pub fn self_usage() -> io::Result<Usage> {
    usage(libc::RUSAGE_SELF)
}

pub fn children_usage() -> io::Result<Usage> {
    usage(libc::RUSAGE_CHILDREN)
}

pub fn terminate(pid: u32) -> io::Result<()> {
    let pid = libc::pid_t::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "child PID exceeds pid_t"))?;
    // SAFETY: `pid` came from `Child::id` and `SIGTERM` is a valid signal number. This is the
    // only signal operation in the crate and targets the owned child process.
    let result = unsafe { libc::kill(pid, libc::SIGTERM) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn usage(who: libc::c_int) -> io::Result<Usage> {
    let mut raw = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `raw` points to writable storage for a `libc::rusage`, and `who` is one of the
    // documented `getrusage` selectors used above. On success the OS initializes all fields.
    let result = unsafe { libc::getrusage(who, raw.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful `getrusage` call above initialized `raw` completely.
    let raw = unsafe { raw.assume_init() };
    Ok(Usage {
        user_seconds: timeval_seconds(raw.ru_utime),
        system_seconds: timeval_seconds(raw.ru_stime),
        max_rss_bytes: max_rss_bytes(raw.ru_maxrss),
    })
}

fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

fn max_rss_bytes(value: libc::c_long) -> u64 {
    let value = u64::try_from(value).unwrap_or_default();
    #[cfg(target_os = "macos")]
    {
        value
    }
    #[cfg(not(target_os = "macos"))]
    {
        value.saturating_mul(1024)
    }
}
