//! Small bounded worker pool for batch convert/restore.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Prefer 2–4 workers: overlap CPU/compress without thrashing disk/RAM.
pub fn default_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .clamp(2, 4)
}

/// Clamp a user-configured worker count to a safe range.
pub fn clamp_pool_size(n: usize) -> usize {
    n.clamp(1, 8)
}

/// Shared progress for a running pool job.
#[derive(Debug)]
pub struct PoolProgress {
    pub total: usize,
    done: AtomicUsize,
    detail: Mutex<String>,
}

impl PoolProgress {
    pub fn new(total: usize) -> Self {
        Self {
            total,
            done: AtomicUsize::new(0),
            detail: Mutex::new(if total == 0 {
                "Nothing to do".into()
            } else {
                "Starting…".into()
            }),
        }
    }

    pub fn done(&self) -> usize {
        self.done.load(Ordering::Relaxed)
    }

    pub fn detail(&self) -> String {
        self.detail
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn mark(&self, done: usize, detail: String) {
        self.done.store(done, Ordering::Relaxed);
        if let Ok(mut d) = self.detail.lock() {
            *d = detail;
        }
    }
}

/// Process `items` with up to `workers` threads. Updates `progress` after each item.
/// `work` returns `(result, detail_for_ui)`.
pub fn run_pool<T, R, F>(
    items: Vec<T>,
    workers: usize,
    progress: Arc<PoolProgress>,
    work: F,
) -> Vec<(R, String)>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> (R, String) + Send + Sync + 'static,
{
    run_pool_cancellable(items, workers, progress, Arc::new(AtomicBool::new(false)), work)
}

/// Like [`run_pool`], but stops taking new items when `cancel` is set.
pub fn run_pool_cancellable<T, R, F>(
    items: Vec<T>,
    workers: usize,
    progress: Arc<PoolProgress>,
    cancel: Arc<AtomicBool>,
    work: F,
) -> Vec<(R, String)>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> (R, String) + Send + Sync + 'static,
{
    let total = items.len();
    if total == 0 {
        return Vec::new();
    }
    let workers = workers.clamp(1, total);
    let queue = Arc::new(Mutex::new(items.into_iter().collect::<VecDeque<T>>()));
    let results: Arc<Mutex<Vec<(R, String)>>> =
        Arc::new(Mutex::new(Vec::with_capacity(total)));
    let work = Arc::new(work);
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let queue = Arc::clone(&queue);
        let results = Arc::clone(&results);
        let progress = Arc::clone(&progress);
        let work = Arc::clone(&work);
        let completed = Arc::clone(&completed);
        let cancel = Arc::clone(&cancel);
        handles.push(thread::spawn(move || {
            loop {
                if cancel.load(Ordering::SeqCst) {
                    // Drop remaining work so other workers exit.
                    let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
                    q.clear();
                    progress.mark(
                        completed.load(Ordering::Relaxed),
                        "Cancelled".into(),
                    );
                    break;
                }
                let item = {
                    let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
                    q.pop_front()
                };
                let Some(item) = item else {
                    break;
                };
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                let (result, detail) = work(item);
                {
                    let mut out = results.lock().unwrap_or_else(|e| e.into_inner());
                    out.push((result, detail.clone()));
                }
                let n = completed.fetch_add(1, Ordering::Relaxed) + 1;
                let label = if cancel.load(Ordering::SeqCst) {
                    format!("Cancelled · last {detail}")
                } else {
                    format!("{n}/{total} {detail}")
                };
                progress.mark(n, label);
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    let mut out = results.lock().unwrap_or_else(|e| e.into_inner());
    std::mem::take(&mut *out)
}

/// Poll `progress` until `done` finishes, calling `on_tick` about every `interval`.
pub fn watch_until<R, F>(
    progress: &PoolProgress,
    interval: Duration,
    done: thread::JoinHandle<R>,
    mut on_tick: F,
) -> R
where
    F: FnMut(usize, usize, String),
{
    let mut last = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap_or_else(Instant::now);
    loop {
        if last.elapsed() >= interval {
            on_tick(progress.done(), progress.total, progress.detail());
            last = Instant::now();
        }
        if done.is_finished() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let result = done.join().expect("pool worker panicked");
    on_tick(progress.done(), progress.total, progress.detail());
    result
}
