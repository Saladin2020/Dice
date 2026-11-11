//! Throughput-aware progress reporting.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub struct ByteProgress {
    started: Instant,
    total_bytes: AtomicU64,
    done_bytes: AtomicU64,
    done_items: AtomicU64,
    total_items: AtomicU64,
}

impl ByteProgress {
    pub fn new(total_items: u64, total_bytes: u64) -> Self {
        Self {
            started: Instant::now(),
            total_bytes: AtomicU64::new(total_bytes),
            done_bytes: AtomicU64::new(0),
            done_items: AtomicU64::new(0),
            total_items: AtomicU64::new(total_items),
        }
    }

    pub fn add_bytes(&self, n: u64) {
        self.done_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_item(&self) {
        self.done_items.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_totals(&self, items: u64, bytes: u64) {
        self.total_items.store(items, Ordering::Relaxed);
        self.total_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ProgressSnapshot {
        let elapsed = self.started.elapsed().as_secs_f64().max(0.001);
        let done_bytes = self.done_bytes.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let done_items = self.done_items.load(Ordering::Relaxed);
        let total_items = self.total_items.load(Ordering::Relaxed);
        let bps = done_bytes as f64 / elapsed;
        ProgressSnapshot {
            done_items,
            total_items,
            done_bytes,
            total_bytes,
            bytes_per_sec: bps,
            elapsed_secs: elapsed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub done_items: u64,
    pub total_items: u64,
    pub done_bytes: u64,
    pub total_bytes: u64,
    pub bytes_per_sec: f64,
    pub elapsed_secs: f64,
}

impl ProgressSnapshot {
    pub fn format_line(&self, detail: &str) -> String {
        format!(
            "{}/{} · {}/{} · {} · {detail}",
            self.done_items,
            self.total_items.max(1),
            format_bytes(self.done_bytes),
            format_bytes(self.total_bytes),
            format_rate(self.bytes_per_sec),
        )
    }
}

pub fn format_bytes(n: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let x = n as f64;
    if x >= GB {
        format!("{:.2} GB", x / GB)
    } else if x >= MB {
        format!("{:.2} MB", x / MB)
    } else if x >= KB {
        format!("{:.1} KB", x / KB)
    } else {
        format!("{n} B")
    }
}

pub fn format_rate(bps: f64) -> String {
    format!("{}/s", format_bytes(bps as u64))
}
