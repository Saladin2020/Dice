use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use eframe::egui;
use egui::{Color32, Frame, Margin, RichText, Sense, Stroke, Vec2};

use crate::assemble::{assemble_file, restore_output_path};
use crate::chunker::ChunkerOptions;
use crate::certs::{default_cert_dir, generate_self_signed};
use crate::exclude::ExcludeSet;
use crate::fs_browser::{default_local_path, expand_selection_filtered, list_dir, FsEntry};
use crate::gc::{delete_manifest, gc_store};
use crate::ingest::ingest_file;
use crate::job::{ConvertJob, JobFile, JobFileStatus};
use crate::manifest::Manifest;
use crate::pool::{clamp_pool_size, run_pool_cancellable, watch_until, PoolProgress};
use crate::settings::{load_settings, save_settings, AppSettings};
use crate::stats::store_stats;
use crate::verify::{verify_restored_file, verify_store};
use crate::store::{init_store, list_manifests};
use crate::transport::{push_over_network_with, push_to_store, serve_store_with, NetOptions};

static JOB_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobStatus {
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueTab {
    Queued,
    Failed,
    Successful,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingsCategory {
    Compression,
    Chunker,
    Workers,
    Transfer,
    Excludes,
    Tls,
    Limits,
}

impl SettingsCategory {
    fn label(self) -> &'static str {
        match self {
            Self::Compression => "Compression",
            Self::Chunker => "Chunker",
            Self::Workers => "Workers",
            Self::Transfer => "Transfer",
            Self::Excludes => "Excludes",
            Self::Tls => "TLS",
            Self::Limits => "Limits",
        }
    }

    fn all() -> [Self; 7] {
        [
            Self::Compression,
            Self::Chunker,
            Self::Workers,
            Self::Transfer,
            Self::Excludes,
            Self::Tls,
            Self::Limits,
        ]
    }
}

#[derive(Debug, Clone)]
struct JobRow {
    id: u64,
    title: String,
    kind: String,
    size_label: String,
    progress: f32,
    status: JobStatus,
    detail: String,
    #[allow(dead_code)]
    started: Instant,
}

enum UiMsg {
    Progress {
        id: u64,
        done: usize,
        total: usize,
        detail: String,
    },
    Finished {
        id: u64,
        ok: bool,
        message: String,
        refresh_store: bool,
        refresh_target: bool,
        refresh_dest: bool,
    },
}

#[derive(Debug, Clone)]
struct StoreRow {
    manifest: Manifest,
    expanded: bool,
}

#[derive(Debug, Clone)]
struct BrowserState {
    path: String,
    entries: Vec<FsEntry>,
    selected: HashSet<PathBuf>,
    error: Option<String>,
}

impl BrowserState {
    fn new(path: PathBuf) -> Self {
        let mut b = Self {
            path: path.display().to_string(),
            entries: Vec::new(),
            selected: HashSet::new(),
            error: None,
        };
        b.reload();
        b
    }

    fn reload(&mut self) {
        match list_dir(&self.path) {
            Ok(entries) => {
                self.entries = entries;
                self.error = None;
                self.selected
                    .retain(|p| self.entries.iter().any(|e| &e.path == p));
            }
            Err(e) => {
                self.entries.clear();
                self.error = Some(e.to_string());
            }
        }
    }

    fn enter(&mut self, path: PathBuf) {
        self.path = path.display().to_string();
        self.selected.clear();
        self.reload();
    }
}

pub struct DiceApp {
    target: BrowserState,
    destination: BrowserState,

    store_path: String,
    store_rows: Vec<StoreRow>,
    store_selected: HashSet<String>,

    remote_store: String,
    send_host: String,
    serve_bind: String,
    serve_stop: Option<Arc<AtomicBool>>,
    serve_port: Option<u16>,

    min_size: String,
    avg_size: String,
    max_size: String,
    zstd_level: String,
    workers: String,
    auth_token: String,
    excludes: String,
    tls_cert: String,
    tls_key: String,
    tls_ca: String,
    send_rate_bps: String,
    verify_restore: bool,

    settings_open: bool,
    settings_category: SettingsCategory,
    settings_error: Option<String>,

    jobs: Vec<JobRow>,
    queue_tab: QueueTab,
    log_lines: Vec<String>,
    status: String,
    cancel_flag: Arc<AtomicBool>,

    /// Relative widths of the 4 panes (must sum ~1.0). Drag splitters to resize.
    pane_fracs: [f32; 4],

    tx: Sender<UiMsg>,
    rx: Receiver<UiMsg>,
    repaint: egui::Context,
}

impl DiceApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        apply_theme(&cc.egui_ctx);
        let (tx, rx) = mpsc::channel();
        let local = default_local_path();
        let saved = load_settings();
        let mut app = Self {
            target: BrowserState::new(local.clone()),
            destination: BrowserState::new(local),
            store_path: default_store_path(),
            store_rows: Vec::new(),
            store_selected: HashSet::new(),
            remote_store: String::new(),
            send_host: saved.send_host.clone(),
            serve_bind: saved.serve_bind.clone(),
            serve_stop: None,
            serve_port: None,
            min_size: saved.min_size.to_string(),
            avg_size: saved.avg_size.to_string(),
            max_size: saved.max_size.to_string(),
            zstd_level: saved.zstd_level.to_string(),
            workers: clamp_pool_size(saved.workers).to_string(),
            auth_token: saved.auth_token.clone(),
            excludes: saved.excludes.clone(),
            tls_cert: saved.tls_cert.clone(),
            tls_key: saved.tls_key.clone(),
            tls_ca: saved.tls_ca.clone(),
            send_rate_bps: saved.send_rate_bps.to_string(),
            verify_restore: saved.verify_restore,
            settings_open: false,
            settings_category: SettingsCategory::Compression,
            settings_error: None,
            jobs: Vec::new(),
            queue_tab: QueueTab::Queued,
            log_lines: vec![
                "Dice ready — Target→Store convert · Store→Destination restore · Transfer send"
                    .into(),
            ],
            status: "Ready".into(),
            cancel_flag: Arc::new(AtomicBool::new(false)),
            pane_fracs: [0.25, 0.25, 0.25, 0.25],
            tx,
            rx,
            repaint: cc.egui_ctx.clone(),
        };
        let _ = app.reload_store();
        app
    }

    fn worker_count(&self) -> usize {
        self.workers
            .parse::<usize>()
            .map(clamp_pool_size)
            .unwrap_or_else(|_| clamp_pool_size(4))
    }

    fn collect_settings(&self) -> Result<AppSettings, String> {
        let settings = AppSettings {
            min_size: self
                .min_size
                .parse()
                .map_err(|_| "Invalid min chunk size".to_string())?,
            avg_size: self
                .avg_size
                .parse()
                .map_err(|_| "Invalid avg chunk size".to_string())?,
            max_size: self
                .max_size
                .parse()
                .map_err(|_| "Invalid max chunk size".to_string())?,
            zstd_level: self
                .zstd_level
                .parse()
                .map_err(|_| "Invalid zstd level".to_string())?,
            workers: self.worker_count(),
            send_host: self.send_host.trim().to_string(),
            serve_bind: self.serve_bind.trim().to_string(),
            auth_token: self.auth_token.clone(),
            excludes: self.excludes.clone(),
            tls_cert: self.tls_cert.trim().to_string(),
            tls_key: self.tls_key.trim().to_string(),
            tls_ca: self.tls_ca.trim().to_string(),
            send_rate_bps: self.send_rate_bps.parse().unwrap_or(0),
            verify_restore: self.verify_restore,
        };
        settings.validate().map_err(|e| e.to_string())?;
        Ok(settings)
    }

    fn apply_settings_values(&mut self, s: &AppSettings) {
        self.min_size = s.min_size.to_string();
        self.avg_size = s.avg_size.to_string();
        self.max_size = s.max_size.to_string();
        self.zstd_level = s.zstd_level.to_string();
        self.workers = clamp_pool_size(s.workers).to_string();
        self.send_host = s.send_host.clone();
        self.serve_bind = s.serve_bind.clone();
        self.auth_token = s.auth_token.clone();
        self.excludes = s.excludes.clone();
        self.tls_cert = s.tls_cert.clone();
        self.tls_key = s.tls_key.clone();
        self.tls_ca = s.tls_ca.clone();
        self.send_rate_bps = s.send_rate_bps.to_string();
        self.verify_restore = s.verify_restore;
    }

    fn request_cancel(&mut self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
        self.status = "Cancelling…".into();
        self.log("Cancel requested — finishing in-flight file then stopping");
    }

    fn begin_batch(&mut self) -> Arc<AtomicBool> {
        self.cancel_flag.store(false, Ordering::SeqCst);
        Arc::clone(&self.cancel_flag)
    }

    fn save_settings_from_ui(&mut self) -> bool {
        match self.collect_settings() {
            Ok(settings) => match save_settings(&settings) {
                Ok(()) => {
                    self.workers = settings.workers.to_string();
                    self.settings_error = None;
                    self.log("Settings saved");
                    self.status = "Settings saved".into();
                    true
                }
                Err(e) => {
                    self.settings_error = Some(e.to_string());
                    false
                }
            },
            Err(e) => {
                self.settings_error = Some(e);
                false
            }
        }
    }

    fn log(&mut self, line: impl Into<String>) {
        self.log_lines.push(line.into());
        if self.log_lines.len() > 400 {
            let drain = self.log_lines.len() - 400;
            self.log_lines.drain(0..drain);
        }
    }

    fn chunker(&self) -> Result<ChunkerOptions, String> {
        Ok(ChunkerOptions {
            min_size: self
                .min_size
                .parse()
                .map_err(|_| "Invalid min size".to_string())?,
            avg_size: self
                .avg_size
                .parse()
                .map_err(|_| "Invalid avg size".to_string())?,
            max_size: self
                .max_size
                .parse()
                .map_err(|_| "Invalid max size".to_string())?,
        })
    }

    fn reload_store(&mut self) -> Result<(), String> {
        init_store(&self.store_path).map_err(|e| e.to_string())?;
        let manifests = list_manifests(&self.store_path).map_err(|e| e.to_string())?;
        let expanded: HashSet<String> = self
            .store_rows
            .iter()
            .filter(|r| r.expanded)
            .map(|r| r.manifest.id.clone())
            .collect();
        self.store_rows = manifests
            .into_iter()
            .map(|manifest| {
                let id = manifest.id.clone();
                StoreRow {
                    expanded: expanded.contains(&id),
                    manifest,
                }
            })
            .collect();
        self.store_selected
            .retain(|id| self.store_rows.iter().any(|r| &r.manifest.id == id));
        Ok(())
    }

    fn spawn_job(
        &mut self,
        title: impl Into<String>,
        kind: impl Into<String>,
        size_label: impl Into<String>,
    ) -> (u64, Sender<UiMsg>, egui::Context) {
        let id = JOB_SEQ.fetch_add(1, Ordering::Relaxed);
        self.jobs.insert(
            0,
            JobRow {
                id,
                title: title.into(),
                kind: kind.into(),
                size_label: size_label.into(),
                progress: 0.0,
                status: JobStatus::Running,
                detail: "Starting…".into(),
                started: Instant::now(),
            },
        );
        self.queue_tab = QueueTab::Queued;
        self.status = "Working…".into();
        (id, self.tx.clone(), self.repaint.clone())
    }

    fn poll_messages(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                UiMsg::Progress {
                    id,
                    done,
                    total,
                    detail,
                } => {
                    if let Some(job) = self.jobs.iter_mut().find(|j| j.id == id) {
                        job.progress = (done as f32 / total.max(1) as f32).clamp(0.0, 1.0);
                        job.detail = detail;
                        job.status = JobStatus::Running;
                    }
                }
                UiMsg::Finished {
                    id,
                    ok,
                    message,
                    refresh_store,
                    refresh_target,
                    refresh_dest,
                } => {
                    if let Some(job) = self.jobs.iter_mut().find(|j| j.id == id) {
                        job.status = if ok {
                            JobStatus::Done
                        } else {
                            JobStatus::Failed
                        };
                        if ok {
                            job.progress = 1.0;
                        }
                        job.detail = message.clone();
                    }
                    self.log(message);
                    if refresh_store {
                        let _ = self.reload_store();
                    }
                    if refresh_target {
                        self.target.reload();
                    }
                    if refresh_dest {
                        self.destination.reload();
                    }
                    let running = self.jobs.iter().any(|j| j.status == JobStatus::Running);
                    self.status = if running {
                        "Working…".into()
                    } else if ok {
                        "Ready".into()
                    } else {
                        "Error".into()
                    };
                }
            }
        }
    }

    fn convert_from_target(&mut self) {
        if self.target.selected.is_empty() {
            self.status = "Select files or folders in Target".into();
            self.log("Convert skipped: nothing selected");
            return;
        }
        let chunker = match self.chunker() {
            Ok(c) => c,
            Err(e) => {
                self.status = e.clone();
                self.log(e);
                return;
            }
        };
        let Ok(level) = self.zstd_level.parse::<i32>() else {
            self.status = "Invalid zstd level".into();
            return;
        };

        let base = PathBuf::from(&self.target.path);
        let selected: Vec<PathBuf> = self.target.selected.iter().cloned().collect();
        let store = self.store_path.clone();
        let excludes_csv = self.excludes.clone();
        let workers = self.worker_count();
        let cancel = self.begin_batch();
        let (id, tx, _ctx) = self.spawn_job(
            "Convert selection",
            "Convert",
            format!("{} item(s)", selected.len()),
        );
        self.log(format!(
            "Convert queued from Target → Store {} (scan + {workers} workers)",
            store
        ));

        thread::spawn(move || {
            let _ = tx.send(UiMsg::Progress {
                id,
                done: 0,
                total: 1,
                detail: "Scanning folders…".into(),
            });

            let excludes = if excludes_csv.trim().is_empty() {
                ExcludeSet::from_defaults()
            } else {
                ExcludeSet::parse_csv(&excludes_csv)
            };
            let files = match expand_selection_filtered(&base, selected, &excludes) {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(UiMsg::Finished {
                        id,
                        ok: false,
                        message: e.to_string(),
                        refresh_store: false,
                        refresh_target: false,
                        refresh_dest: false,
                    });
                    return;
                }
            };
            if files.is_empty() {
                let _ = tx.send(UiMsg::Finished {
                    id,
                    ok: false,
                    message: "No files under selection (empty folders are skipped)".into(),
                    refresh_store: false,
                    refresh_target: false,
                    refresh_dest: false,
                });
                return;
            }

            let mut job = ConvertJob {
                version: 1,
                store: store.clone(),
                base: base.display().to_string(),
                zstd_level: level,
                min_size: chunker.min_size,
                avg_size: chunker.avg_size,
                max_size: chunker.max_size,
                workers,
                files: files
                    .iter()
                    .map(|f| JobFile {
                        absolute: f.path.display().to_string(),
                        relative_path: f.relative_path.clone(),
                        size: f.size,
                        status: JobFileStatus::Pending,
                        error: String::new(),
                        manifest_id: String::new(),
                    })
                    .collect(),
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            let journal = ConvertJob::journal_path(&store);
            let _ = job.save(&journal);

            let total_files = files.len();
            let progress = Arc::new(PoolProgress::new(total_files));
            let progress_watch = Arc::clone(&progress);
            let store_w = store.clone();
            let cancel_w = Arc::clone(&cancel);
            let handle = thread::spawn(move || {
                run_pool_cancellable(files, workers, progress, cancel_w, move |file| {
                    let rel = file.relative_path.clone();
                    let outcome = ingest_file(
                        &store_w,
                        &file.path,
                        Some(&file.relative_path),
                        chunker,
                        level,
                        |_, _| {},
                    );
                    (outcome, rel)
                })
            });

            let outcomes = watch_until(
                &progress_watch,
                std::time::Duration::from_millis(80),
                handle,
                |done, total, detail| {
                    let _ = tx.send(UiMsg::Progress {
                        id,
                        done,
                        total,
                        detail,
                    });
                },
            );

            let mut ok_n = 0usize;
            let mut skip_n = 0usize;
            let mut fail_n = 0usize;
            let mut new_chunks = 0usize;
            let mut reused_chunks = 0usize;
            let mut raw_bytes = 0u64;
            let mut stored_bytes = 0u64;
            for (outcome, rel) in outcomes {
                match outcome {
                    Ok(r) if r.skipped => {
                        skip_n += 1;
                        job.mark(&rel, JobFileStatus::Skipped, r.manifest.id, "");
                    }
                    Ok(r) => {
                        ok_n += 1;
                        new_chunks += r.new_chunks;
                        reused_chunks += r.reused_chunks;
                        raw_bytes += r.raw_bytes;
                        stored_bytes += r.stored_bytes;
                        job.mark(&rel, JobFileStatus::Done, r.manifest.id, "");
                    }
                    Err(e) => {
                        fail_n += 1;
                        job.mark(&rel, JobFileStatus::Failed, "", e.to_string());
                        let _ = tx.send(UiMsg::Progress {
                            id,
                            done: ok_n + skip_n + fail_n,
                            total: total_files,
                            detail: format!("Failed {rel}: {e}"),
                        });
                    }
                }
            }
            let cancelled = cancel.load(Ordering::SeqCst);
            let _ = job.save(&journal);
            if fail_n == 0 && !cancelled {
                let _ = ConvertJob::clear_journal(&store);
            }

            let saved = raw_bytes.saturating_sub(stored_bytes);
            let ok = fail_n == 0 && !cancelled;
            let message = if cancelled {
                format!(
                    "Cancelled · {ok_n} ok, {skip_n} skipped, {fail_n} failed — resume available"
                )
            } else if fail_n == 0 {
                format!(
                    "Converted {ok_n} (+{skip_n} skipped) · new {new_chunks} · reused {reused_chunks} · saved ~{}",
                    format_bytes(saved)
                )
            } else {
                format!(
                    "Converted {ok_n} ok, {fail_n} failed · new {new_chunks} · reused {reused_chunks}"
                )
            };
            let _ = tx.send(UiMsg::Finished {
                id,
                ok,
                message,
                refresh_store: ok_n > 0 || skip_n > 0,
                refresh_target: false,
                refresh_dest: false,
            });
        });
    }

    fn resume_convert(&mut self) {
        let store = PathBuf::from(&self.store_path);
        let journal = ConvertJob::journal_path(&store);
        let mut job = match ConvertJob::load(&journal) {
            Ok(j) => j,
            Err(e) => {
                self.status = format!("No resume journal: {e}");
                self.log(self.status.clone());
                return;
            }
        };
        for f in &mut job.files {
            if f.status == JobFileStatus::Failed {
                f.status = JobFileStatus::Pending;
                f.error.clear();
            }
        }
        let pending: Vec<JobFile> = job
            .files
            .iter()
            .filter(|f| f.status == JobFileStatus::Pending)
            .cloned()
            .collect();
        if pending.is_empty() {
            self.status = "Nothing to resume".into();
            let _ = ConvertJob::clear_journal(&store);
            return;
        }
        let chunker = ChunkerOptions {
            min_size: job.min_size,
            avg_size: job.avg_size,
            max_size: job.max_size,
        };
        let level = job.zstd_level;
        let workers = clamp_pool_size(job.workers);
        let cancel = self.begin_batch();
        let (id, tx, _ctx) = self.spawn_job(
            "Resume convert",
            "Convert",
            format!("{} pending", pending.len()),
        );
        self.log(format!("Resuming {} file(s) from journal", pending.len()));
        let store_s = store.display().to_string();

        thread::spawn(move || {
            let total_files = pending.len();
            let progress = Arc::new(PoolProgress::new(total_files));
            let progress_watch = Arc::clone(&progress);
            let store_w = store_s.clone();
            let cancel_w = Arc::clone(&cancel);
            let handle = thread::spawn(move || {
                run_pool_cancellable(pending, workers, progress, cancel_w, move |file| {
                    let rel = file.relative_path.clone();
                    let outcome = ingest_file(
                        &store_w,
                        PathBuf::from(&file.absolute),
                        Some(&file.relative_path),
                        chunker,
                        level,
                        |_, _| {},
                    );
                    (outcome, rel)
                })
            });
            let outcomes = watch_until(
                &progress_watch,
                std::time::Duration::from_millis(80),
                handle,
                |done, total, detail| {
                    let _ = tx.send(UiMsg::Progress {
                        id,
                        done,
                        total,
                        detail,
                    });
                },
            );
            let mut ok_n = 0usize;
            let mut fail_n = 0usize;
            for (outcome, rel) in outcomes {
                match outcome {
                    Ok(r) => {
                        ok_n += 1;
                        job.mark(
                            &rel,
                            if r.skipped {
                                JobFileStatus::Skipped
                            } else {
                                JobFileStatus::Done
                            },
                            r.manifest.id,
                            "",
                        );
                    }
                    Err(e) => {
                        fail_n += 1;
                        job.mark(&rel, JobFileStatus::Failed, "", e.to_string());
                    }
                }
            }
            let cancelled = cancel.load(Ordering::SeqCst);
            let _ = job.save(&journal);
            if fail_n == 0 && !cancelled && job.pending_count() == 0 {
                let _ = ConvertJob::clear_journal(&store_s);
            }
            let _ = tx.send(UiMsg::Finished {
                id,
                ok: fail_n == 0 && !cancelled,
                message: if cancelled {
                    format!("Resume cancelled · {ok_n} done — journal kept")
                } else {
                    format!("Resume finished · {ok_n} ok, {fail_n} failed")
                },
                refresh_store: ok_n > 0,
                refresh_target: false,
                refresh_dest: false,
            });
        });
    }

    fn restore_to_destination(&mut self) {
        let selected: Vec<Manifest> = self
            .store_rows
            .iter()
            .filter(|r| self.store_selected.contains(&r.manifest.id))
            .map(|r| r.manifest.clone())
            .collect();
        if selected.is_empty() {
            self.status = "Select files in Dice store".into();
            self.log("Restore skipped: no store files selected");
            return;
        }
        let dest_dir = PathBuf::from(&self.destination.path);
        if !dest_dir.is_dir() {
            self.status = "Destination is not a folder".into();
            return;
        }
        let store = self.store_path.clone();
        let total_files = selected.len();
        let workers = self.worker_count();
        let verify = self.verify_restore;
        let cancel = self.begin_batch();
        let (id, tx, _ctx) = self.spawn_job(
            "Restore selection",
            "Restore",
            format!("{total_files} file(s)"),
        );
        self.log(format!(
            "Restore {total_files} file(s) → Destination {} ({workers} workers)",
            dest_dir.display()
        ));

        thread::spawn(move || {
            let progress = Arc::new(PoolProgress::new(total_files));
            let progress_watch = Arc::clone(&progress);
            let cancel_w = Arc::clone(&cancel);
            let handle = thread::spawn(move || {
                run_pool_cancellable(selected, workers, progress, cancel_w, move |m| {
                    let logical = m.logical_path().to_string();
                    let hash = m.hash.clone();
                    let result = (|| {
                        let out = restore_output_path(&dest_dir, &m)?;
                        assemble_file(&store, &m.id, &out, |_, _| {})?;
                        if verify {
                            verify_restored_file(&out, &hash)?;
                        }
                        Ok::<(), crate::error::DiceError>(())
                    })();
                    (result, logical)
                })
            });

            let outcomes = watch_until(
                &progress_watch,
                std::time::Duration::from_millis(80),
                handle,
                |done, total, detail| {
                    let _ = tx.send(UiMsg::Progress {
                        id,
                        done,
                        total,
                        detail,
                    });
                },
            );

            let mut ok_n = 0usize;
            let mut fail_n = 0usize;
            for (outcome, rel) in outcomes {
                match outcome {
                    Ok(()) => ok_n += 1,
                    Err(e) => {
                        fail_n += 1;
                        let _ = tx.send(UiMsg::Progress {
                            id,
                            done: ok_n + fail_n,
                            total: total_files,
                            detail: format!("Failed {rel}: {e}"),
                        });
                    }
                }
            }

            let cancelled = cancel.load(Ordering::SeqCst);
            let ok = fail_n == 0 && !cancelled;
            let message = if cancelled {
                format!("Restore cancelled · {ok_n} ok, {fail_n} failed")
            } else if fail_n == 0 {
                format!(
                    "Restored {ok_n} file(s) · {workers} workers{}",
                    if verify { " · verified" } else { "" }
                )
            } else {
                format!("Restored {ok_n} ok, {fail_n} failed · {workers} workers")
            };
            let _ = tx.send(UiMsg::Finished {
                id,
                ok,
                message,
                refresh_store: false,
                refresh_target: false,
                refresh_dest: ok_n > 0,
            });
        });
    }

    fn push_selected(&mut self) {
        if self.remote_store.trim().is_empty() {
            self.status = "Set remote store folder in Transfer".into();
            return;
        }
        let selected: Vec<Manifest> = self
            .store_rows
            .iter()
            .filter(|r| self.store_selected.contains(&r.manifest.id))
            .map(|r| r.manifest.clone())
            .collect();
        if selected.is_empty() {
            self.status = "Select store files to transfer".into();
            return;
        }
        let store = self.store_path.clone();
        let remote = self.remote_store.clone();
        for m in selected {
            let (id, tx, ctx) = self.spawn_job(m.name.clone(), "Push", format_bytes(m.size));
            let store = store.clone();
            let remote = remote.clone();
            let mid = m.id.clone();
            thread::spawn(move || {
                let result = push_to_store(&store, &remote, &mid, |done, total| {
                    let _ = tx.send(UiMsg::Progress {
                        id,
                        done,
                        total,
                        detail: format!("Chunk {done}/{total}"),
                    });
                    ctx.request_repaint();
                });
                match result {
                    Ok(s) => {
                        let _ = tx.send(UiMsg::Finished {
                            id,
                            ok: true,
                            message: format!(
                                "Pushed {} · sent {} · reused {}",
                                s.manifest_id, s.sent_chunks, s.reused_chunks
                            ),
                            refresh_store: false,
                            refresh_target: false,
                            refresh_dest: false,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(UiMsg::Finished {
                            id,
                            ok: false,
                            message: e.to_string(),
                            refresh_store: false,
                            refresh_target: false,
                            refresh_dest: false,
                        });
                    }
                }
                ctx.request_repaint();
            });
        }
    }

    fn send_selected(&mut self) {
        let (host, port) = match parse_target(&self.send_host) {
            Ok(v) => v,
            Err(e) => {
                self.status = e;
                return;
            }
        };
        let selected: Vec<Manifest> = self
            .store_rows
            .iter()
            .filter(|r| self.store_selected.contains(&r.manifest.id))
            .map(|r| r.manifest.clone())
            .collect();
        if selected.is_empty() {
            self.status = "Select store files to send".into();
            return;
        }
        let store = self.store_path.clone();
        let token = if self.auth_token.trim().is_empty() {
            None
        } else {
            Some(self.auth_token.clone())
        };
        let rate = self.send_rate_bps.parse().unwrap_or(0);
        let tls_ca = if self.tls_ca.trim().is_empty() {
            None
        } else {
            Some(PathBuf::from(self.tls_ca.trim()))
        };
        let use_tls = tls_ca.is_some()
            || (!self.tls_cert.trim().is_empty() && !self.tls_key.trim().is_empty());
        for m in selected {
            let (id, tx, ctx) = self.spawn_job(m.name.clone(), "Send", format_bytes(m.size));
            let store = store.clone();
            let host = host.clone();
            let mid = m.id.clone();
            let opts = NetOptions {
                token: token.clone(),
                tls_ca: tls_ca.clone(),
                use_tls,
                rate_limit_bps: rate,
                ..NetOptions::default()
            };
            thread::spawn(move || {
                let result =
                    push_over_network_with(&store, &host, port, &mid, opts, |done, total| {
                        let _ = tx.send(UiMsg::Progress {
                            id,
                            done,
                            total,
                            detail: format!("Chunk {done}/{total}"),
                        });
                        ctx.request_repaint();
                    });
                match result {
                    Ok(s) => {
                        let _ = tx.send(UiMsg::Finished {
                            id,
                            ok: true,
                            message: format!(
                                "Sent {} · chunks {} · reused {}",
                                s.manifest_id, s.sent_chunks, s.reused_chunks
                            ),
                            refresh_store: false,
                            refresh_target: false,
                            refresh_dest: false,
                        });
                    }
                    Err(e) => {
                        let _ = tx.send(UiMsg::Finished {
                            id,
                            ok: false,
                            message: e.to_string(),
                            refresh_store: false,
                            refresh_target: false,
                            refresh_dest: false,
                        });
                    }
                }
                ctx.request_repaint();
            });
        }
    }
}

impl eframe::App for DiceApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_messages();

        egui::TopBottomPanel::top("menubar")
            .exact_height(28.0)
            .frame(
                Frame::new()
                    .fill(Color32::from_rgb(24, 24, 24))
                    .inner_margin(Margin::symmetric(8, 2)),
            )
            .show(ctx, |ui| {
                egui::menu::bar(ui, |ui| {
                    ui.menu_button("Dice", |ui| {
                        if ui.button("Settings…").clicked() {
                            self.settings_open = true;
                            self.settings_error = None;
                            ui.close_menu();
                        }
                    });
                    ui.menu_button("Help", |ui| {
                        ui.label("Convert nested folders from Target");
                        ui.label("Restore keeps relative paths");
                        ui.label("Settings: Dice → Settings…");
                    });
                });
            });

        egui::TopBottomPanel::top("header")
            .exact_height(44.0)
            .frame(
                Frame::new()
                    .fill(Color32::from_rgb(32, 32, 32))
                    .inner_margin(Margin::symmetric(12, 8)),
            )
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.heading(RichText::new("Dice").color(Color32::WHITE).size(20.0));
                    ui.label(
                        RichText::new("Target · Store · Destination · Transfer")
                            .color(Color32::from_rgb(180, 180, 180))
                            .size(13.0),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .button(RichText::new("Settings").color(Color32::WHITE))
                            .on_hover_text("Open settings")
                            .clicked()
                        {
                            self.settings_open = true;
                            self.settings_error = None;
                        }
                        let running = self
                            .jobs
                            .iter()
                            .filter(|j| j.status == JobStatus::Running)
                            .count();
                        ui.label(
                            RichText::new(if running > 0 {
                                format!("{running} job(s) running")
                            } else {
                                self.status.clone()
                            })
                            .color(Color32::from_rgb(125, 211, 252)),
                        );
                    });
                });
            });

        self.ui_settings_window(ctx);

        egui::TopBottomPanel::top("messages")
            .default_height(40.0)
            .min_height(28.0)
            .max_height(120.0)
            .resizable(true)
            .frame(
                Frame::new()
                    .fill(Color32::from_rgb(252, 252, 252))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(210, 210, 210)))
                    .inner_margin(Margin::symmetric(8, 4)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Message log").strong().size(12.0));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.small_button("Clear log").clicked() {
                            self.log_lines.clear();
                        }
                    });
                });
                egui::ScrollArea::vertical()
                    .stick_to_bottom(true)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for line in &self.log_lines {
                            ui.add(
                                egui::Label::new(RichText::new(line).monospace().size(11.0))
                                    .truncate(),
                            );
                        }
                    });
            });

        egui::TopBottomPanel::bottom("queue")
            .default_height(108.0)
            .min_height(80.0)
            .max_height(220.0)
            .resizable(true)
            .frame(
                Frame::new()
                    .fill(Color32::WHITE)
                    .stroke(Stroke::new(1.0, Color32::from_rgb(210, 210, 210)))
                    .inner_margin(Margin::symmetric(8, 6)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    queue_tab_btn(ui, &mut self.queue_tab, QueueTab::Queued, "Queued");
                    queue_tab_btn(ui, &mut self.queue_tab, QueueTab::Failed, "Failed");
                    queue_tab_btn(ui, &mut self.queue_tab, QueueTab::Successful, "Successful");
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("Clear finished").clicked() {
                            self.jobs.retain(|j| j.status == JobStatus::Running);
                        }
                        let running = self.jobs.iter().any(|j| j.status == JobStatus::Running);
                        if ui
                            .add_enabled(running, egui::Button::new("Cancel"))
                            .on_hover_text("Stop after the current file")
                            .clicked()
                        {
                            self.request_cancel();
                        }
                        if ui
                            .button("Resume")
                            .on_hover_text("Resume interrupted convert from store journal")
                            .clicked()
                        {
                            self.resume_convert();
                        }
                    });
                });
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let filtered: Vec<&JobRow> = self
                            .jobs
                            .iter()
                            .filter(|j| match self.queue_tab {
                                QueueTab::Queued => j.status == JobStatus::Running,
                                QueueTab::Failed => j.status == JobStatus::Failed,
                                QueueTab::Successful => j.status == JobStatus::Done,
                            })
                            .collect();
                        if filtered.is_empty() {
                            ui.label(
                                RichText::new("No items")
                                    .color(Color32::from_rgb(140, 140, 140)),
                            );
                        }
                        for job in filtered {
                            job_row(ui, job);
                            ui.add_space(3.0);
                        }
                    });
            });

        egui::CentralPanel::default()
            .frame(Frame::new().fill(Color32::from_rgb(200, 200, 200)).inner_margin(Margin::ZERO))
            .show(ctx, |ui| {
                self.show_resizable_panes(ui);
            });

        if self.jobs.iter().any(|j| j.status == JobStatus::Running)
            || self.serve_stop.is_some()
            || ctx.input(|i| i.pointer.any_down())
        {
            ctx.request_repaint_after(std::time::Duration::from_millis(33));
        }
    }
}

impl DiceApp {
    fn show_resizable_panes(&mut self, ui: &mut egui::Ui) {
        let full = ui.available_rect_before_wrap();
        let splitter_w = 5.0;
        let min_frac = 0.12;
        let titles: [(&str, &str); 4] = [
            ("1 · Target", "Convert source"),
            ("2 · Dice store", "Chunk storage"),
            ("3 · Destination", "Restore output"),
            ("4 · Transfer", "Push / send"),
        ];

        // Keep fractions valid.
        let sum: f32 = self.pane_fracs.iter().sum::<f32>().max(0.0001);
        for f in &mut self.pane_fracs {
            *f /= sum;
        }

        let usable = (full.width() - splitter_w * 3.0).max(4.0 * 80.0);
        let widths = [
            usable * self.pane_fracs[0],
            usable * self.pane_fracs[1],
            usable * self.pane_fracs[2],
            usable * self.pane_fracs[3],
        ];

        let mut x = full.min.x;
        for i in 0..4 {
            let pane_rect = egui::Rect::from_min_size(
                egui::pos2(x, full.min.y),
                Vec2::new(widths[i], full.height()),
            );

            // Pane background (flush, no card gap).
            ui.painter()
                .rect_filled(pane_rect, 0.0, Color32::from_rgb(248, 248, 248));

            ui.allocate_new_ui(
                egui::UiBuilder::new()
                    .max_rect(pane_rect)
                    .layout(egui::Layout::top_down(egui::Align::Min)),
                |ui| {
                    ui.set_clip_rect(pane_rect);
                    pane_shell(ui, titles[i].0, titles[i].1, |ui| match i {
                        0 => self.ui_browser_zone(ui, ZoneKind::Target),
                        1 => self.ui_store_zone(ui),
                        2 => self.ui_browser_zone(ui, ZoneKind::Destination),
                        _ => self.ui_transfer_zone(ui),
                    });
                },
            );

            x += widths[i];

            if i < 3 {
                let split_rect = egui::Rect::from_min_size(
                    egui::pos2(x, full.min.y),
                    Vec2::new(splitter_w, full.height()),
                );
                let id = ui.id().with(("pane_split", i));
                let resp = ui.interact(split_rect, id, Sense::click_and_drag());

                let hot = resp.hovered() || resp.dragged();
                ui.painter().rect_filled(
                    split_rect,
                    0.0,
                    if hot {
                        Color32::from_rgb(56, 189, 248)
                    } else {
                        Color32::from_rgb(160, 160, 160)
                    },
                );
                // Grip line
                let mid = split_rect.center().x;
                ui.painter().vline(
                    mid,
                    split_rect.y_range(),
                    Stroke::new(1.0, Color32::from_rgb(90, 90, 90)),
                );

                if hot {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);
                }

                if resp.dragged() {
                    let delta = resp.drag_delta().x / usable;
                    let left = self.pane_fracs[i];
                    let right = self.pane_fracs[i + 1];
                    let new_left = (left + delta).clamp(min_frac, left + right - min_frac);
                    let new_right = left + right - new_left;
                    if new_right >= min_frac {
                        self.pane_fracs[i] = new_left;
                        self.pane_fracs[i + 1] = new_right;
                    }
                }

                x += splitter_w;
            }
        }

        ui.allocate_rect(full, Sense::hover());
    }
}

#[derive(Clone, Copy)]
enum ZoneKind {
    Target,
    Destination,
}

impl DiceApp {
    fn ui_browser_zone(&mut self, ui: &mut egui::Ui, kind: ZoneKind) {
        let (action_label, action_hint) = match kind {
            ZoneKind::Target => (
                "Convert → Store",
                "Convert selected files or folders (nested paths preserved)",
            ),
            ZoneKind::Destination => (
                "← Restore here",
                "Restore selected store files here (nested folders recreated)",
            ),
        };

        {
            let browser = match kind {
                ZoneKind::Target => &mut self.target,
                ZoneKind::Destination => &mut self.destination,
            };

            let enter = ui
                .add(
                    egui::TextEdit::singleline(&mut browser.path)
                        .desired_width(ui.available_width())
                        .hint_text("Folder path")
                        .clip_text(true),
                )
                .lost_focus()
                && ui.input(|i| i.key_pressed(egui::Key::Enter));
            if enter {
                browser.reload();
            }

            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing.x = 4.0;
                if ui.small_button("Up").clicked() {
                    let path = PathBuf::from(&browser.path);
                    if let Some(parent) = path.parent() {
                        browser.enter(parent.to_path_buf());
                    }
                }
                if ui.small_button("Browse").clicked() {
                    if let Some(p) = rfd::FileDialog::new().pick_folder() {
                        browser.enter(p);
                    }
                }
                if ui.small_button("Refresh").clicked() {
                    browser.reload();
                }
                ui.label(
                    RichText::new(format!("{} sel", browser.selected.len()))
                        .size(11.0)
                        .color(Color32::from_rgb(100, 116, 139)),
                );
            });

            if let Some(err) = &browser.error {
                ui.colored_label(Color32::from_rgb(185, 28, 28), err.clone());
            }

            ui.add_space(4.0);
            let mut navigate: Option<PathBuf> = None;
            let mut toggle: Option<(PathBuf, bool)> = None;
            let list_h = (ui.available_height() - 36.0).max(80.0);

            egui::ScrollArea::vertical()
                .id_salt(match kind {
                    ZoneKind::Target => "target_scroll",
                    ZoneKind::Destination => "dest_scroll",
                })
                .auto_shrink([false, false])
                .max_height(list_h)
                .show(ui, |ui| {
                    let name_w = (ui.available_width() - 72.0).max(80.0);
                    egui::Grid::new(match kind {
                        ZoneKind::Target => "target_grid",
                        ZoneKind::Destination => "dest_grid",
                    })
                    .num_columns(3)
                    .spacing([4.0, 2.0])
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("");
                        ui.strong("Name");
                        ui.strong("Size");
                        ui.end_row();

                        if let Some(parent) =
                            PathBuf::from(&browser.path).parent().map(|p| p.to_path_buf())
                        {
                            ui.label("");
                            if ui
                                .add(egui::Label::new("..").sense(Sense::click()).truncate())
                                .clicked()
                            {
                                navigate = Some(parent);
                            }
                            ui.label("");
                            ui.end_row();
                        }

                        for entry in &browser.entries {
                            let mut checked = browser.selected.contains(&entry.path);
                            // Target: folders selectable (recursive convert). Destination: files only.
                            let allow_select = match kind {
                                ZoneKind::Target => true,
                                ZoneKind::Destination => !entry.is_dir,
                            };
                            if allow_select {
                                if ui.checkbox(&mut checked, "").changed() {
                                    toggle = Some((entry.path.clone(), checked));
                                }
                            } else {
                                ui.label("");
                            }

                            let name = if entry.is_dir {
                                format!("/ {}", entry.name)
                            } else {
                                entry.name.clone()
                            };
                            let resp = ui
                                .allocate_ui_with_layout(
                                    Vec2::new(name_w, 18.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.add(
                                            egui::Label::new(name)
                                                .sense(Sense::click())
                                                .truncate(),
                                        )
                                    },
                                )
                                .inner;
                            if entry.is_dir && resp.clicked() {
                                navigate = Some(entry.path.clone());
                            } else if !entry.is_dir && resp.clicked() {
                                toggle = Some((entry.path.clone(), !checked));
                            }

                            ui.label(if entry.is_dir {
                                String::new()
                            } else {
                                format_bytes(entry.size)
                            });
                            ui.end_row();
                        }
                    });
                });

            if let Some(path) = navigate {
                browser.enter(path);
            }
            if let Some((path, on)) = toggle {
                if on {
                    browser.selected.insert(path);
                } else {
                    browser.selected.remove(&path);
                }
            }
        }

        ui.add_space(6.0);
        let accent = match kind {
            ZoneKind::Target => Color32::from_rgb(14, 165, 233),
            ZoneKind::Destination => Color32::from_rgb(16, 185, 129),
        };
        if ui
            .add_sized(
                [ui.available_width(), 30.0],
                egui::Button::new(RichText::new(action_label).color(Color32::WHITE).strong())
                    .fill(accent),
            )
            .on_hover_text(action_hint)
            .clicked()
        {
            match kind {
                ZoneKind::Target => self.convert_from_target(),
                ZoneKind::Destination => self.restore_to_destination(),
            }
        }
    }

    fn ui_store_zone(&mut self, ui: &mut egui::Ui) {
        ui.add(
            egui::TextEdit::singleline(&mut self.store_path)
                .desired_width(ui.available_width())
                .clip_text(true)
                .hint_text("Store folder"),
        );

        ui.horizontal_wrapped(|ui| {
            ui.spacing_mut().item_spacing.x = 4.0;
            if ui.small_button("Browse").clicked() {
                if let Some(p) = rfd::FileDialog::new().pick_folder() {
                    self.store_path = p.display().to_string();
                    let _ = self.reload_store();
                }
            }
            if ui.small_button("Open").clicked() {
                match self.reload_store() {
                    Ok(()) => {
                        self.status = "Store ready".into();
                        self.log(format!("Opened store {}", self.store_path));
                    }
                    Err(e) => {
                        self.status = e.clone();
                        self.log(e);
                    }
                }
            }
            if ui.small_button("Refresh").clicked() {
                match self.reload_store() {
                    Ok(()) => self.log("Store refreshed"),
                    Err(e) => self.log(e),
                }
            }
            if ui.small_button("All").clicked() {
                for row in &self.store_rows {
                    self.store_selected.insert(row.manifest.id.clone());
                }
            }
            if ui.small_button("None").clicked() {
                self.store_selected.clear();
            }
            if ui.small_button("Verify").clicked() {
                match verify_store(&self.store_path) {
                    Ok(r) => {
                        let msg = format!(
                            "Verify: {} ok / {} manifests · {} issues",
                            r.ok,
                            r.manifests,
                            r.issues.len()
                        );
                        self.status = msg.clone();
                        self.log(msg);
                        for issue in r.issues.into_iter().take(20) {
                            self.log(format!(
                                "  ! {} — {}",
                                issue.path, issue.message
                            ));
                        }
                    }
                    Err(e) => {
                        self.status = e.to_string();
                        self.log(e.to_string());
                    }
                }
            }
            if ui.small_button("Delete").clicked() {
                let ids: Vec<String> = self.store_selected.iter().cloned().collect();
                if ids.is_empty() {
                    self.status = "Select store files to delete".into();
                } else {
                    for id in ids {
                        match delete_manifest(&self.store_path, &id) {
                            Ok(r) => self.log(format!(
                                "Deleted {} (chunks removed {})",
                                r.manifest_id, r.deleted_chunks
                            )),
                            Err(e) => self.log(format!("Delete {id}: {e}")),
                        }
                    }
                    let _ = self.reload_store();
                }
            }
            if ui.small_button("GC").clicked() {
                match gc_store(&self.store_path) {
                    Ok(r) => {
                        let msg = format!(
                            "GC deleted {} / scanned {}",
                            r.deleted_chunks, r.scanned_chunks
                        );
                        self.status = msg.clone();
                        self.log(msg);
                    }
                    Err(e) => {
                        self.status = e.to_string();
                        self.log(e.to_string());
                    }
                }
            }
            if ui.small_button("Stats").clicked() {
                match store_stats(&self.store_path) {
                    Ok(s) => {
                        let msg = s.format_summary();
                        self.status = msg.clone();
                        self.log(msg);
                    }
                    Err(e) => {
                        self.status = e.to_string();
                        self.log(e.to_string());
                    }
                }
            }
            if ui.small_button("Resume").clicked() {
                self.resume_convert();
            }
        });
        ui.label(
            RichText::new(format!(
                "{} files · {} selected",
                self.store_rows.len(),
                self.store_selected.len()
            ))
            .size(11.0)
            .color(Color32::from_rgb(100, 116, 139)),
        );

        ui.add_space(4.0);
        let mut toggle: Option<(String, bool)> = None;
        let mut toggle_expand: Option<String> = None;
        let list_h = (ui.available_height() - 52.0).max(80.0);

        egui::ScrollArea::vertical()
            .id_salt("store_scroll")
            .auto_shrink([false, false])
            .max_height(list_h)
            .show(ui, |ui| {
                let name_w = (ui.available_width() - 72.0).max(80.0);
                egui::Grid::new("store_grid")
                    .num_columns(3)
                    .spacing([4.0, 2.0])
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("");
                        ui.strong("Name");
                        ui.strong("Size");
                        ui.end_row();

                        if self.store_rows.is_empty() {
                            ui.label("");
                            ui.label(
                                RichText::new("Empty — convert from Target")
                                    .color(Color32::GRAY),
                            );
                            ui.end_row();
                        }

                        for row in &self.store_rows {
                            let m = &row.manifest;
                            let mut checked = self.store_selected.contains(&m.id);
                            if ui.checkbox(&mut checked, "").changed() {
                                toggle = Some((m.id.clone(), checked));
                            }
                            let marker = if row.expanded { "▼" } else { "▶" };
                            let label = format!("{marker} {}", m.logical_path());
                            let resp = ui
                                .allocate_ui_with_layout(
                                    Vec2::new(name_w, 18.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        ui.add(
                                            egui::Label::new(RichText::new(label).strong())
                                                .sense(Sense::click())
                                                .truncate(),
                                        )
                                    },
                                )
                                .inner;
                            if resp.clicked() {
                                toggle_expand = Some(m.id.clone());
                            }
                            ui.label(format!("{}·{}", format_bytes(m.size), m.chunks.len()));
                            ui.end_row();

                            if row.expanded {
                                for (i, chunk) in m.chunks.iter().enumerate() {
                                    ui.label("");
                                    ui.add(
                                        egui::Label::new(
                                            RichText::new(format!(
                                                "  #{i:04} {}",
                                                short_hash(&chunk.hash)
                                            ))
                                            .monospace()
                                            .size(10.0)
                                            .color(Color32::DARK_GRAY),
                                        )
                                        .truncate(),
                                    );
                                    ui.label(format_bytes(chunk.raw_size as u64));
                                    ui.end_row();
                                }
                            }
                        }
                    });
            });

        if let Some((id, on)) = toggle {
            if on {
                self.store_selected.insert(id);
            } else {
                self.store_selected.remove(&id);
            }
        }
        if let Some(id) = toggle_expand {
            if let Some(row) = self.store_rows.iter_mut().find(|r| r.manifest.id == id) {
                row.expanded = !row.expanded;
            }
        }

        ui.add_space(4.0);
        ui.label(
            RichText::new(format!(
                "Chunk {}/{}/{} · zstd {} · {} workers  (Dice → Settings)",
                format_bytes(self.min_size.parse::<u64>().unwrap_or(0)),
                format_bytes(self.avg_size.parse::<u64>().unwrap_or(0)),
                format_bytes(self.max_size.parse::<u64>().unwrap_or(0)),
                self.zstd_level,
                self.worker_count()
            ))
            .size(11.0)
            .color(Color32::from_rgb(100, 116, 139)),
        );
    }

    fn ui_settings_window(&mut self, ctx: &egui::Context) {
        if !self.settings_open {
            return;
        }
        let mut open = true;
        let mut request_close = false;
        egui::Window::new("Settings")
            .id(egui::Id::new("dice_settings_v3"))
            .open(&mut open)
            .collapsible(false)
            .resizable(true)
            .default_size([700.0, 360.0])
            .min_width(620.0)
            .min_height(300.0)
            .max_height(420.0)
            .constrain(true)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .frame(
                Frame::window(&ctx.style())
                    .fill(Color32::from_rgb(240, 240, 240))
                    .inner_margin(Margin::symmetric(8, 6)),
            )
            .show(ctx, |ui| {
                // Reserve footer first so Cancel/Save never clip.
                let footer_reserve = 42.0;
                let body_h = (ui.available_height() - footer_reserve).clamp(200.0, 340.0);
                let cat_w = 160.0;
                let border = Color32::from_rgb(128, 128, 128);

                ui.allocate_ui_with_layout(
                    Vec2::new(ui.available_width(), body_h),
                    egui::Layout::left_to_right(egui::Align::TOP),
                    |ui| {
                        // Left: fixed-width Category list (PuTTY-style)
                        ui.allocate_ui_with_layout(
                            Vec2::new(cat_w, body_h),
                            egui::Layout::top_down(egui::Align::Min).with_cross_justify(true),
                            |ui| {
                                ui.label(RichText::new("Category").strong().size(12.5));
                                ui.add_space(3.0);
                                Frame::new()
                                    .fill(Color32::WHITE)
                                    .stroke(Stroke::new(1.0, border))
                                    .inner_margin(Margin::symmetric(2, 2))
                                    .show(ui, |ui| {
                                        let list_h = (body_h - 22.0).max(160.0);
                                        ui.set_width(cat_w - 6.0);
                                        ui.set_min_height(list_h);
                                        ui.set_max_height(list_h);
                                        for cat in SettingsCategory::all() {
                                            let selected = self.settings_category == cat;
                                            let row = Vec2::new(ui.available_width().max(120.0), 20.0);
                                            let label = if selected {
                                                RichText::new(cat.label()).strong()
                                            } else {
                                                RichText::new(cat.label())
                                            };
                                            let resp = ui.add_sized(
                                                row,
                                                egui::SelectableLabel::new(selected, label),
                                            );
                                            if resp.clicked() {
                                                self.settings_category = cat;
                                            }
                                        }
                                        let leftover = ui.available_height();
                                        if leftover > 0.0 {
                                            ui.allocate_exact_size(
                                                Vec2::new(ui.available_width(), leftover),
                                                Sense::hover(),
                                            );
                                        }
                                    });
                            },
                        );

                        ui.add_space(10.0);

                        // Right: options for selected category
                        ui.allocate_ui_with_layout(
                            Vec2::new(ui.available_width(), body_h),
                            egui::Layout::top_down(egui::Align::Min).with_cross_justify(true),
                            |ui| {
                                Frame::new()
                                    .fill(Color32::from_rgb(240, 240, 240))
                                    .stroke(Stroke::new(1.0, border))
                                    .inner_margin(Margin::symmetric(10, 8))
                                    .show(ui, |ui| {
                                        ui.set_height(body_h - 2.0);
                                        ui.set_width(ui.available_width());
                                        egui::ScrollArea::vertical()
                                            .id_salt("settings_panel")
                                            .max_height(body_h - 18.0)
                                            .auto_shrink([false, false])
                                            .show(ui, |ui| {
                                                ui.set_min_width(
                                                    (ui.available_width() - 4.0).max(380.0),
                                                );
                                                match self.settings_category {
                                                    SettingsCategory::Compression => {
                                                        self.ui_settings_compression(ui);
                                                    }
                                                    SettingsCategory::Chunker => {
                                                        self.ui_settings_chunker(ui);
                                                    }
                                                    SettingsCategory::Workers => {
                                                        self.ui_settings_workers(ui);
                                                    }
                                                    SettingsCategory::Transfer => {
                                                        self.ui_settings_transfer(ui);
                                                    }
                                                    SettingsCategory::Excludes => {
                                                        self.ui_settings_excludes(ui);
                                                    }
                                                    SettingsCategory::Tls => {
                                                        self.ui_settings_tls(ui);
                                                    }
                                                    SettingsCategory::Limits => {
                                                        self.ui_settings_limits(ui);
                                                    }
                                                }
                                            });
                                    });
                            },
                        );
                    },
                );

                if let Some(err) = &self.settings_error {
                    ui.colored_label(Color32::from_rgb(185, 28, 28), err.clone());
                }

                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    if ui
                        .add(egui::Button::new("Reset defaults").min_size(Vec2::new(110.0, 24.0)))
                        .clicked()
                    {
                        let defaults = AppSettings::default();
                        self.apply_settings_values(&defaults);
                        self.settings_error = None;
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add(egui::Button::new("Save").min_size(Vec2::new(80.0, 24.0)))
                            .clicked()
                        {
                            if self.save_settings_from_ui() {
                                request_close = true;
                            }
                        }
                        ui.add_space(6.0);
                        if ui
                            .add(egui::Button::new("Cancel").min_size(Vec2::new(80.0, 24.0)))
                            .clicked()
                        {
                            let saved = load_settings();
                            self.apply_settings_values(&saved);
                            self.settings_error = None;
                            request_close = true;
                        }
                    });
                });
            });
        if request_close || !open {
            self.settings_open = false;
        }
    }

    fn ui_settings_compression(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "zstd compression", |ui| {
            ui.label("1 = fastest · higher = smaller / slower");
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label("Level");
                ui.add_space(8.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.zstd_level)
                        .desired_width(64.0)
                        .hint_text("1–22"),
                );
            });
        });
    }

    fn ui_settings_chunker(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "FastCDC chunk sizes (bytes)", |ui| {
            ui.label("Require min ≤ avg ≤ max.");
            ui.add_space(8.0);
            egui::Grid::new("settings_chunker_grid")
                .num_columns(3)
                .spacing([14.0, 8.0])
                .min_col_width(40.0)
                .show(ui, |ui| {
                    ui.label("Min");
                    ui.add(egui::TextEdit::singleline(&mut self.min_size).desired_width(140.0));
                    ui.label(
                        RichText::new(format_bytes(self.min_size.parse::<u64>().unwrap_or(0)))
                            .color(Color32::DARK_GRAY),
                    );
                    ui.end_row();
                    ui.label("Avg");
                    ui.add(egui::TextEdit::singleline(&mut self.avg_size).desired_width(140.0));
                    ui.label(
                        RichText::new(format_bytes(self.avg_size.parse::<u64>().unwrap_or(0)))
                            .color(Color32::DARK_GRAY),
                    );
                    ui.end_row();
                    ui.label("Max");
                    ui.add(egui::TextEdit::singleline(&mut self.max_size).desired_width(140.0));
                    ui.label(
                        RichText::new(format_bytes(self.max_size.parse::<u64>().unwrap_or(0)))
                            .color(Color32::DARK_GRAY),
                    );
                    ui.end_row();
                });
        });
        settings_group(ui, "Presets", |ui| {
            ui.horizontal(|ui| {
                if ui
                    .add(egui::Button::new("Speed").min_size(Vec2::new(96.0, 24.0)))
                    .clicked()
                {
                    self.min_size = (256 * 1024).to_string();
                    self.avg_size = (1024 * 1024).to_string();
                    self.max_size = (4 * 1024 * 1024).to_string();
                    self.zstd_level = "1".into();
                }
                if ui
                    .add(egui::Button::new("Balanced").min_size(Vec2::new(96.0, 24.0)))
                    .clicked()
                {
                    self.min_size = (512 * 1024).to_string();
                    self.avg_size = (2 * 1024 * 1024).to_string();
                    self.max_size = (8 * 1024 * 1024).to_string();
                    self.zstd_level = "3".into();
                }
                if ui
                    .add(egui::Button::new("Small").min_size(Vec2::new(96.0, 24.0)))
                    .clicked()
                {
                    self.min_size = (128 * 1024).to_string();
                    self.avg_size = (512 * 1024).to_string();
                    self.max_size = (2 * 1024 * 1024).to_string();
                    self.zstd_level = "5".into();
                }
            });
        });
    }

    fn ui_settings_workers(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "Parallel convert / restore threads", |ui| {
            ui.horizontal(|ui| {
                ui.label("Pool size");
                ui.add_space(8.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.workers)
                        .desired_width(56.0)
                        .hint_text("1–8"),
                );
            });
        });
    }

    fn ui_settings_transfer(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "Network defaults", |ui| {
            egui::Grid::new("settings_transfer_grid")
                .num_columns(2)
                .spacing([10.0, 8.0])
                .show(ui, |ui| {
                    ui.label("Send host");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.send_host)
                            .desired_width(260.0)
                            .hint_text("127.0.0.1:7788"),
                    );
                    ui.end_row();
                    ui.label("Serve bind");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.serve_bind)
                            .desired_width(260.0)
                            .hint_text("0.0.0.0:7788"),
                    );
                    ui.end_row();
                    ui.label("Auth token");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.auth_token)
                            .desired_width(260.0)
                            .password(true)
                            .hint_text("optional shared secret"),
                    );
                    ui.end_row();
                });
        });
    }

    fn ui_settings_excludes(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "Skip names when converting trees", |ui| {
            ui.label("Comma-separated folder/file names.");
            ui.add(
                egui::TextEdit::multiline(&mut self.excludes)
                    .desired_width(ui.available_width())
                    .desired_rows(8),
            );
        });
    }

    fn ui_settings_tls(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "Certificate files", |ui| {
            egui::Grid::new("settings_tls_grid")
                .num_columns(3)
                .spacing([8.0, 8.0])
                .show(ui, |ui| {
                    ui.label("Cert");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.tls_cert)
                            .desired_width(240.0)
                            .hint_text("path to cert.pem"),
                    );
                    if ui.button("Browse…").clicked() {
                        if let Some(p) = rfd::FileDialog::new()
                            .add_filter("PEM", &["pem", "crt", "cer"])
                            .pick_file()
                        {
                            self.tls_cert = p.display().to_string();
                        }
                    }
                    ui.end_row();
                    ui.label("Key");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.tls_key)
                            .desired_width(240.0)
                            .hint_text("path to key.pem"),
                    );
                    if ui.button("Browse…").clicked() {
                        if let Some(p) = rfd::FileDialog::new()
                            .add_filter("PEM", &["pem", "key"])
                            .pick_file()
                        {
                            self.tls_key = p.display().to_string();
                        }
                    }
                    ui.end_row();
                    ui.label("CA");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.tls_ca)
                            .desired_width(240.0)
                            .hint_text("optional CA for client"),
                    );
                    if ui.button("Browse…").clicked() {
                        if let Some(p) = rfd::FileDialog::new()
                            .add_filter("PEM", &["pem", "crt"])
                            .pick_file()
                        {
                            self.tls_ca = p.display().to_string();
                        }
                    }
                    ui.end_row();
                });
        });
        ui.add_space(8.0);
        settings_group(ui, "Generate", |ui| {
            ui.label("Create a self-signed cert for local/lab use.");
            if ui.button("Generate self-signed").clicked() {
                match generate_self_signed(default_cert_dir(), &["localhost", "127.0.0.1"]) {
                    Ok(g) => {
                        self.tls_cert = g.cert_path.display().to_string();
                        self.tls_key = g.key_path.display().to_string();
                        self.tls_ca = g.cert_path.display().to_string();
                        self.settings_error = None;
                        self.log(format!("Generated TLS certs in {}", g.dir.display()));
                    }
                    Err(e) => self.settings_error = Some(e.to_string()),
                }
            }
            ui.label(
                RichText::new("Saved under AppData\\Dice\\tls")
                    .size(11.0)
                    .color(Color32::GRAY),
            );
        });
    }

    fn ui_settings_limits(&mut self, ui: &mut egui::Ui) {
        settings_group(ui, "Send bandwidth", |ui| {
            ui.label("0 = unlimited.");
            ui.horizontal(|ui| {
                ui.label("Send rate (B/s)");
                ui.add(
                    egui::TextEdit::singleline(&mut self.send_rate_bps)
                        .desired_width(120.0)
                        .hint_text("0"),
                );
            });
        });
        ui.add_space(8.0);
        settings_group(ui, "Restore integrity", |ui| {
            ui.checkbox(&mut self.verify_restore, "Verify hash after restore");
        });
    }

    fn ui_transfer_zone(&mut self, ui: &mut egui::Ui) {
        ui.label(
            RichText::new(format!(
                "{} store file(s) selected",
                self.store_selected.len()
            ))
            .size(12.0)
            .color(Color32::from_rgb(100, 116, 139)),
        );
        ui.add_space(8.0);

        ui.label(RichText::new("Local push").strong().size(13.0));
        ui.add(
            egui::TextEdit::singleline(&mut self.remote_store)
                .desired_width(ui.available_width())
                .hint_text("Remote store folder")
                .clip_text(true),
        );
        ui.horizontal_wrapped(|ui| {
            if ui.small_button("Browse").clicked() {
                if let Some(p) = rfd::FileDialog::new().pick_folder() {
                    self.remote_store = p.display().to_string();
                }
            }
        });
        if ui
            .add_sized(
                [ui.available_width(), 28.0],
                egui::Button::new(RichText::new("Push selected").color(Color32::WHITE).strong())
                    .fill(Color32::from_rgb(14, 165, 233)),
            )
            .clicked()
        {
            self.push_selected();
        }

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(8.0);

        ui.label(RichText::new("Network").strong().size(13.0));
        ui.label(RichText::new("Serve").size(11.0).color(Color32::GRAY));
        ui.horizontal_wrapped(|ui| {
            ui.add(
                egui::TextEdit::singleline(&mut self.serve_bind)
                    .desired_width(120.0)
                    .clip_text(true),
            );
            if self.serve_stop.is_none() {
                if ui.small_button("Start").clicked() {
                    let stop = Arc::new(AtomicBool::new(false));
                    let opts = NetOptions {
                        token: if self.auth_token.trim().is_empty() {
                            None
                        } else {
                            Some(self.auth_token.clone())
                        },
                        tls_cert: if self.tls_cert.trim().is_empty() {
                            None
                        } else {
                            Some(PathBuf::from(self.tls_cert.trim()))
                        },
                        tls_key: if self.tls_key.trim().is_empty() {
                            None
                        } else {
                            Some(PathBuf::from(self.tls_key.trim()))
                        },
                        ..NetOptions::default()
                    };
                    match serve_store_with(
                        self.store_path.clone(),
                        &self.serve_bind,
                        stop.clone(),
                        opts,
                    ) {
                        Ok(port) => {
                            self.serve_stop = Some(stop);
                            self.serve_port = Some(port);
                            self.status = format!("Serving :{port}");
                            self.log(format!("Serving on {}", self.serve_bind));
                        }
                        Err(e) => {
                            self.status = e.to_string();
                            self.log(e.to_string());
                        }
                    }
                }
            } else if ui.small_button("Stop").clicked() {
                if let Some(stop) = self.serve_stop.take() {
                    stop.store(true, Ordering::SeqCst);
                }
                self.serve_port = None;
                self.status = "Serve stopped".into();
                self.log("Serve stopped");
            }
            if let Some(port) = self.serve_port {
                ui.label(
                    RichText::new(format!(":{port}"))
                        .color(Color32::from_rgb(22, 163, 74)),
                );
            }
        });

        ui.add_space(6.0);
        ui.label(RichText::new("Send to").size(11.0).color(Color32::GRAY));
        ui.add(
            egui::TextEdit::singleline(&mut self.send_host)
                .desired_width(ui.available_width())
                .hint_text("host:port")
                .clip_text(true),
        );
        if ui
            .add_sized(
                [ui.available_width(), 28.0],
                egui::Button::new(RichText::new("Send selected").color(Color32::WHITE).strong())
                    .fill(Color32::from_rgb(99, 102, 241)),
            )
            .clicked()
        {
            self.send_selected();
        }

        ui.add_space(10.0);
        ui.label(
            RichText::new("Only missing chunks are transferred.")
                .size(11.0)
                .color(Color32::GRAY),
        );
    }
}

fn pane_shell(
    ui: &mut egui::Ui,
    title: &str,
    subtitle: &str,
    add: impl FnOnce(&mut egui::Ui),
) {
    // Flush pane: header strip + body (no floating card).
    let header_h = 36.0;
    let full = ui.max_rect();
    let header = egui::Rect::from_min_size(full.min, Vec2::new(full.width(), header_h));
    ui.painter()
        .rect_filled(header, 0.0, Color32::from_rgb(235, 235, 235));
    ui.painter().hline(
        header.x_range(),
        header.bottom(),
        Stroke::new(1.0, Color32::from_rgb(180, 180, 180)),
    );

    ui.allocate_new_ui(
        egui::UiBuilder::new()
            .max_rect(header.shrink2(Vec2::new(8.0, 4.0)))
            .layout(egui::Layout::left_to_right(egui::Align::Center)),
        |ui| {
            ui.vertical(|ui| {
                ui.label(
                    RichText::new(title)
                        .strong()
                        .size(13.0)
                        .color(Color32::from_rgb(30, 30, 30)),
                );
                ui.label(
                    RichText::new(subtitle)
                        .size(10.0)
                        .color(Color32::from_rgb(100, 100, 100)),
                );
            });
        },
    );

    let body = egui::Rect::from_min_max(
        egui::pos2(full.min.x, header.bottom()),
        full.max,
    );
    ui.painter()
        .rect_filled(body, 0.0, Color32::from_rgb(248, 248, 248));
    ui.allocate_new_ui(
        egui::UiBuilder::new()
            .max_rect(body.shrink2(Vec2::new(8.0, 6.0)))
            .layout(egui::Layout::top_down(egui::Align::Min)),
        |ui| {
            ui.set_clip_rect(body);
            add(ui);
        },
    );
}

fn queue_tab_btn(ui: &mut egui::Ui, current: &mut QueueTab, tab: QueueTab, label: &str) {
    if ui.selectable_label(*current == tab, label).clicked() {
        *current = tab;
    }
}

fn job_row(ui: &mut egui::Ui, job: &JobRow) {
    let color = match job.status {
        JobStatus::Running => Color32::from_rgb(2, 132, 199),
        JobStatus::Done => Color32::from_rgb(22, 163, 74),
        JobStatus::Failed => Color32::from_rgb(220, 38, 38),
    };
    Frame::new()
        .fill(Color32::from_rgb(250, 250, 250))
        .stroke(Stroke::new(1.0, Color32::from_rgb(220, 220, 220)))
        .inner_margin(Margin::symmetric(8, 5))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new(&job.title).strong());
                ui.label(RichText::new(&job.kind).color(Color32::GRAY));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(RichText::new(&job.size_label).color(color));
                });
            });
            ui.add(
                egui::ProgressBar::new(job.progress)
                    .show_percentage()
                    .animate(job.status == JobStatus::Running)
                    .desired_height(12.0),
            );
            ui.label(RichText::new(&job.detail).size(11.0).color(Color32::DARK_GRAY));
        });
}

fn settings_group(ui: &mut egui::Ui, title: &str, add_contents: impl FnOnce(&mut egui::Ui)) {
    // PuTTY-style group box: title sits above a bordered panel
    ui.label(RichText::new(title).strong().size(12.5));
    ui.add_space(2.0);
    Frame::new()
        .fill(Color32::from_rgb(240, 240, 240))
        .stroke(Stroke::new(1.0, Color32::from_rgb(160, 160, 160)))
        .inner_margin(Margin::symmetric(12, 10))
        .corner_radius(2.0)
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            add_contents(ui);
        });
    ui.add_space(10.0);
}

fn apply_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::light();
    visuals.panel_fill = Color32::from_rgb(245, 245, 245);
    visuals.window_fill = Color32::WHITE;
    visuals.widgets.inactive.bg_fill = Color32::from_rgb(235, 235, 235);
    visuals.selection.bg_fill = Color32::from_rgb(186, 230, 253);
    ctx.set_visuals(visuals);
}

fn default_store_path() -> String {
    if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        return PathBuf::from(home)
            .join(".dice-store")
            .display()
            .to_string();
    }
    "./.dice-store".into()
}

fn parse_target(target: &str) -> Result<(String, u16), String> {
    let (host, port) = target
        .rsplit_once(':')
        .ok_or_else(|| "Target must be host:port".to_string())?;
    let port: u16 = port.parse().map_err(|_| "Invalid port".to_string())?;
    Ok((host.to_string(), port))
}

fn format_bytes(n: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let n = n as f64;
    if n < KB {
        format!("{n:.0} B")
    } else if n < MB {
        format!("{:.1} KB", n / KB)
    } else {
        format!("{:.2} MB", n / MB)
    }
}

fn short_hash(hash: &str) -> String {
    if hash.len() <= 16 {
        hash.to_string()
    } else {
        format!("{}…{}", &hash[..8], &hash[hash.len() - 6..])
    }
}
