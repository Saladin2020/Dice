//! Simple token-bucket style send rate limiter.

use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RateLimiter {
    bytes_per_sec: u64,
    window_start: Instant,
    sent_in_window: u64,
}

impl RateLimiter {
    /// `bytes_per_sec == 0` means unlimited.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            window_start: Instant::now(),
            sent_in_window: 0,
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0)
    }

    pub fn take(&mut self, nbytes: u64) {
        if self.bytes_per_sec == 0 || nbytes == 0 {
            return;
        }
        let mut remaining = nbytes;
        while remaining > 0 {
            let elapsed = self.window_start.elapsed();
            if elapsed >= Duration::from_secs(1) {
                self.window_start = Instant::now();
                self.sent_in_window = 0;
            }
            let room = self.bytes_per_sec.saturating_sub(self.sent_in_window);
            if room == 0 {
                let sleep_for = Duration::from_secs(1).saturating_sub(elapsed);
                thread::sleep(sleep_for.max(Duration::from_millis(1)));
                continue;
            }
            let chunk = remaining.min(room);
            self.sent_in_window += chunk;
            remaining -= chunk;
        }
    }
}
