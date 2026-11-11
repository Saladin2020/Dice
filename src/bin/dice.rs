//! Cross-platform Dice CLI (Windows / macOS / Linux).
//!
//! CLI-only build:
//!   cargo build --release --bin dice --no-default-features
//!
//! Exit codes: 0 ok · 1 error · 2 verify issues · 3 partial failure

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::{Parser, Subcommand};
use dice_desktop::assemble::{assemble_file, restore_output_path};
use dice_desktop::chunker::ChunkerOptions;
use dice_desktop::exclude::ExcludeSet;
use dice_desktop::fs_browser::expand_selection_filtered;
use dice_desktop::gc::{delete_manifest, gc_store};
use dice_desktop::ingest::ingest_file;
use dice_desktop::job::{append_log, ConvertJob, JobFile, JobFileStatus};
use dice_desktop::pool::{clamp_pool_size, run_pool, PoolProgress};
use dice_desktop::progress::{format_bytes, ByteProgress};
use dice_desktop::store::{init_store, list_manifests};
use dice_desktop::transport::{
    push_over_network_with, push_to_store, serve_store_with, NetOptions,
};
use dice_desktop::verify::verify_store;

const EXIT_OK: i32 = 0;
const EXIT_ERROR: i32 = 1;
const EXIT_VERIFY: i32 = 2;
const EXIT_PARTIAL: i32 = 3;

#[derive(Parser, Debug)]
#[command(
    name = "dice",
    version,
    about = "Dice — CDC chunk store with compression and dedup sync"
)]
struct Cli {
    /// Append logs to this file
    #[arg(long, global = true)]
    log: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init {
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
    },
    Put {
        paths: Vec<PathBuf>,
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
        #[arg(long)]
        base: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        zstd: i32,
        #[arg(long, default_value_t = 262144)]
        min: u32,
        #[arg(long, default_value_t = 1048576)]
        avg: u32,
        #[arg(long, default_value_t = 4194304)]
        max: u32,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Comma-separated exclude names (default: .git,node_modules,...)
        #[arg(long)]
        exclude: Option<String>,
        /// Resume incomplete convert journal in the store
        #[arg(long)]
        resume: bool,
    },
    List {
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
    },
    Restore {
        query: String,
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Skip post-restore content hash check
        #[arg(long)]
        no_verify: bool,
    },
    /// Show store size / dedup summary
    Stats {
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
    },
    Verify {
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
    },
    Rm {
        query: String,
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
    },
    Gc {
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
    },
    Push {
        query: String,
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
        #[arg(long)]
        remote: PathBuf,
    },
    Send {
        query: String,
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7788")]
        to: String,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        tls: bool,
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        /// Max send rate in bytes/sec (0 = unlimited)
        #[arg(long, default_value_t = 0)]
        rate: u64,
    },
    Serve {
        #[arg(long, default_value = ".dice-store")]
        store: PathBuf,
        #[arg(long, default_value = "0.0.0.0:7788")]
        bind: String,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
    },
}

fn main() {
    let code = match run() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            EXIT_ERROR
        }
    };
    std::process::exit(code);
}

fn run() -> Result<i32, String> {
    let cli = Cli::parse();
    let log = cli.log.clone();
    let log_line = |msg: &str| {
        println!("{msg}");
        if let Some(path) = &log {
            let _ = append_log(path, msg);
        }
    };

    match cli.command {
        Commands::Init { store } => {
            init_store(&store).map_err(err)?;
            log_line(&format!("store ready: {}", store.display()));
            Ok(EXIT_OK)
        }
        Commands::Put {
            paths,
            store,
            base,
            zstd,
            min,
            avg,
            max,
            workers,
            exclude,
            resume,
        } => cmd_put(
            paths, store, base, zstd, min, avg, max, workers, exclude, resume, &log_line,
        ),
        Commands::List { store } => {
            cmd_list(store)?;
            Ok(EXIT_OK)
        }
        Commands::Restore {
            query,
            store,
            out,
            workers,
            no_verify,
        } => cmd_restore(query, store, out, workers, !no_verify, &log_line),
        Commands::Stats { store } => {
            let s = dice_desktop::stats::store_stats(&store).map_err(err)?;
            log_line(&s.format_summary());
            Ok(EXIT_OK)
        }
        Commands::Verify { store } => cmd_verify(store, &log_line),
        Commands::Rm { query, store } => {
            let r = delete_manifest(&store, &query).map_err(err)?;
            log_line(&format!(
                "deleted {} · dropped refs {} · removed chunks {}",
                r.manifest_id, r.dropped_refs, r.deleted_chunks
            ));
            Ok(EXIT_OK)
        }
        Commands::Gc { store } => {
            let r = gc_store(&store).map_err(err)?;
            log_line(&format!(
                "gc scanned {} · deleted {} · kept {}",
                r.scanned_chunks, r.deleted_chunks, r.kept_chunks
            ));
            Ok(EXIT_OK)
        }
        Commands::Push {
            query,
            store,
            remote,
        } => cmd_push(query, store, remote, &log_line),
        Commands::Send {
            query,
            store,
            to,
            token,
            tls,
            tls_ca,
            rate,
        } => cmd_send(query, store, to, token, tls, tls_ca, rate, &log_line),
        Commands::Serve {
            store,
            bind,
            token,
            tls_cert,
            tls_key,
        } => cmd_serve(store, bind, token, tls_cert, tls_key, &log_line),
    }
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn chunker(min: u32, avg: u32, max: u32) -> Result<ChunkerOptions, String> {
    if min < 64 || avg < min || max < avg {
        return Err("require 64 ≤ min ≤ avg ≤ max".into());
    }
    Ok(ChunkerOptions {
        min_size: min,
        avg_size: avg,
        max_size: max,
    })
}

fn cmd_put(
    paths: Vec<PathBuf>,
    store: PathBuf,
    base: Option<PathBuf>,
    zstd: i32,
    min: u32,
    avg: u32,
    max: u32,
    workers: usize,
    exclude: Option<String>,
    resume: bool,
    log_line: &dyn Fn(&str),
) -> Result<i32, String> {
    if !(1..=22).contains(&zstd) {
        return Err("zstd level must be 1–22".into());
    }
    let chunker = chunker(min, avg, max)?;
    let workers = clamp_pool_size(workers);
    init_store(&store).map_err(err)?;

    let journal_path = ConvertJob::journal_path(&store);
    let mut job = if resume && journal_path.exists() {
        ConvertJob::load(&journal_path).map_err(err)?
    } else {
        if paths.is_empty() {
            return Err("provide at least one file or folder (or --resume)".into());
        }
        let excludes = exclude
            .map(|s| ExcludeSet::parse_csv(&s))
            .unwrap_or_else(ExcludeSet::from_defaults);
        let base = resolve_base(&paths, base)?;
        let files = expand_selection_filtered(&base, paths, &excludes).map_err(err)?;
        if files.is_empty() {
            return Err("no files found under selection".into());
        }
        ConvertJob {
            version: 1,
            store: store.display().to_string(),
            base: base.display().to_string(),
            zstd_level: zstd,
            min_size: min,
            avg_size: avg,
            max_size: max,
            workers,
            files: files
                .into_iter()
                .map(|f| JobFile {
                    absolute: f.path.display().to_string(),
                    relative_path: f.relative_path,
                    size: f.size,
                    status: JobFileStatus::Pending,
                    error: String::new(),
                    manifest_id: String::new(),
                })
                .collect(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    };

    // Re-queue failed entries on resume.
    for f in &mut job.files {
        if f.status == JobFileStatus::Failed {
            f.status = JobFileStatus::Pending;
            f.error.clear();
        }
    }
    job.save(&journal_path).map_err(err)?;

    let pending: Vec<JobFile> = job
        .files
        .iter()
        .filter(|f| f.status == JobFileStatus::Pending)
        .cloned()
        .collect();
    let total_bytes: u64 = pending.iter().map(|f| f.size).sum();
    let bytes = Arc::new(ByteProgress::new(pending.len() as u64, total_bytes));
    log_line(&format!(
        "put {} pending file(s) · {} → {} ({} workers, zstd {zstd})",
        pending.len(),
        format_bytes(total_bytes),
        store.display(),
        workers
    ));

    let progress = Arc::new(PoolProgress::new(pending.len()));
    let progress_watch = Arc::clone(&progress);
    let bytes_watch = Arc::clone(&bytes);
    let bytes_for_workers = Arc::clone(&bytes);
    let store_w = store.clone();
    let handle = thread::spawn(move || {
        run_pool(pending, workers, progress, move |file| {
            let rel = file.relative_path.clone();
            let outcome = ingest_file(
                &store_w,
                Path::new(&file.absolute),
                Some(&file.relative_path),
                chunker,
                zstd,
                |_, _| {},
            );
            if let Ok(ref r) = outcome {
                bytes_for_workers.add_bytes(r.raw_bytes);
            }
            bytes_for_workers.add_item();
            (outcome, rel)
        })
    });

    while !handle.is_finished() {
        let snap = bytes_watch.snapshot();
        eprint!(
            "\r{}",
            snap.format_line(&progress_watch.detail())
        );
        let _ = std::io::Write::flush(&mut std::io::stderr());
        thread::sleep(Duration::from_millis(80));
    }
    let outcomes = handle.join().map_err(|_| "worker panicked".to_string())?;
    eprintln!();

    let mut ok_n = 0usize;
    let mut skip_n = 0usize;
    let mut fail_n = 0usize;
    let mut new_chunks = 0usize;
    let mut reused = 0usize;

    for (outcome, rel) in &outcomes {
        match outcome {
            Ok(r) if r.skipped => {
                skip_n += 1;
                job.mark(rel, JobFileStatus::Skipped, r.manifest.id.clone(), "");
            }
            Ok(r) => {
                ok_n += 1;
                new_chunks += r.new_chunks;
                reused += r.reused_chunks;
                job.mark(rel, JobFileStatus::Done, r.manifest.id.clone(), "");
                log_line(&format!(
                    "  + {rel}  (chunks {}, new {}, reused {})",
                    r.manifest.chunks.len(),
                    r.new_chunks,
                    r.reused_chunks
                ));
            }
            Err(e) => {
                fail_n += 1;
                job.mark(rel, JobFileStatus::Failed, "", e.to_string());
                log_line(&format!("  ! {rel}: {e}"));
            }
        }
    }

    job.save(&journal_path).map_err(err)?;
    if fail_n == 0 {
        let _ = ConvertJob::clear_journal(&store);
    }

    let snap = bytes.snapshot();
    log_line(&format!(
        "done: {ok_n} ok, {skip_n} skipped, {fail_n} failed · new {new_chunks} · reused {reused} · {} · {}",
        format_bytes(snap.done_bytes),
        dice_desktop::progress::format_rate(snap.bytes_per_sec)
    ));

    if fail_n > 0 {
        Ok(EXIT_PARTIAL)
    } else {
        Ok(EXIT_OK)
    }
}

fn resolve_base(paths: &[PathBuf], base: Option<PathBuf>) -> Result<PathBuf, String> {
    if let Some(b) = base {
        return Ok(b);
    }
    if paths.len() == 1 {
        let p = &paths[0];
        return Ok(p
            .parent()
            .map(|x| x.to_path_buf())
            .unwrap_or_else(|| PathBuf::from(".")));
    }
    std::env::current_dir().map_err(err)
}

fn cmd_list(store: PathBuf) -> Result<(), String> {
    init_store(&store).map_err(err)?;
    let manifests = list_manifests(&store).map_err(err)?;
    if manifests.is_empty() {
        println!("(empty store) {}", store.display());
        return Ok(());
    }
    for m in manifests {
        println!(
            "{}\t{}\t{}\t{}",
            m.id,
            m.logical_path(),
            m.size,
            m.chunks.len()
        );
    }
    Ok(())
}

fn resolve_manifests(store: &Path, query: &str) -> Result<Vec<String>, String> {
    let all = list_manifests(store).map_err(err)?;
    if query.eq_ignore_ascii_case("all") {
        return Ok(all.into_iter().map(|m| m.id).collect());
    }
    let exact: Vec<_> = all
        .iter()
        .filter(|m| m.id.eq_ignore_ascii_case(query))
        .cloned()
        .collect();
    if exact.len() == 1 {
        return Ok(vec![exact[0].id.clone()]);
    }
    let q = query.to_lowercase();
    let matches: Vec<_> = all
        .into_iter()
        .filter(|m| {
            m.id.to_lowercase().contains(&q)
                || m.name.to_lowercase().contains(&q)
                || m.logical_path().to_lowercase().contains(&q)
        })
        .collect();
    if matches.is_empty() {
        return Err(format!("manifest not found: {query}"));
    }
    if matches.len() > 1 {
        eprintln!("ambiguous \"{query}\":");
        for m in &matches {
            eprintln!("  {}  {}", m.id, m.logical_path());
        }
        return Err("ambiguous manifest query".into());
    }
    Ok(vec![matches[0].id.clone()])
}

fn cmd_restore(
    query: String,
    store: PathBuf,
    out: PathBuf,
    workers: usize,
    verify: bool,
    log_line: &dyn Fn(&str),
) -> Result<i32, String> {
    init_store(&store).map_err(err)?;
    std::fs::create_dir_all(&out).map_err(err)?;
    let ids = resolve_manifests(&store, &query)?;
    let selected: Vec<_> = list_manifests(&store)
        .map_err(err)?
        .into_iter()
        .filter(|m| ids.iter().any(|id| id == &m.id))
        .collect();
    let workers = clamp_pool_size(workers);
    let total_bytes: u64 = selected.iter().map(|m| m.size).sum();
    let bytes = Arc::new(ByteProgress::new(selected.len() as u64, total_bytes));
    log_line(&format!(
        "restore {} file(s) · {} → {}",
        selected.len(),
        format_bytes(total_bytes),
        out.display()
    ));

    let progress = Arc::new(PoolProgress::new(selected.len()));
    let progress_watch = Arc::clone(&progress);
    let bytes_watch = Arc::clone(&bytes);
    let bytes_for_workers = Arc::clone(&bytes);
    let handle = thread::spawn(move || {
        run_pool(selected, workers, progress, move |m| {
            let logical = m.logical_path().to_string();
            let size = m.size;
            let result = (|| {
                let dest = restore_output_path(&out, &m)?;
                let hash = m.hash.clone();
                assemble_file(&store, &m.id, &dest, |_, _| {})?;
                if verify {
                    dice_desktop::verify::verify_restored_file(&dest, &hash)?;
                }
                Ok::<(), dice_desktop::error::DiceError>(())
            })();
            if result.is_ok() {
                bytes_for_workers.add_bytes(size);
            }
            bytes_for_workers.add_item();
            (result, logical)
        })
    });
    while !handle.is_finished() {
        eprint!(
            "\r{}",
            bytes_watch
                .snapshot()
                .format_line(&progress_watch.detail())
        );
        let _ = std::io::Write::flush(&mut std::io::stderr());
        thread::sleep(Duration::from_millis(80));
    }
    let outcomes = handle.join().map_err(|_| "worker panicked".to_string())?;
    eprintln!();

    let mut fail = 0usize;
    for (outcome, rel) in outcomes {
        if let Err(e) = outcome {
            fail += 1;
            log_line(&format!("  ! {rel}: {e}"));
        }
    }
    if fail > 0 {
        Ok(EXIT_PARTIAL)
    } else {
        log_line("restored OK");
        Ok(EXIT_OK)
    }
}

fn cmd_verify(store: PathBuf, log_line: &dyn Fn(&str)) -> Result<i32, String> {
    init_store(&store).map_err(err)?;
    let report = verify_store(&store).map_err(err)?;
    log_line(&format!(
        "verify manifests {} · ok {} · chunks checked {} · issues {}",
        report.manifests,
        report.ok,
        report.chunks_checked,
        report.issues.len()
    ));
    for issue in &report.issues {
        log_line(&format!(
            "  ! {} ({}) — {}",
            issue.path, issue.manifest_id, issue.message
        ));
    }
    if report.issues.is_empty() {
        Ok(EXIT_OK)
    } else {
        Ok(EXIT_VERIFY)
    }
}

fn cmd_push(
    query: String,
    store: PathBuf,
    remote: PathBuf,
    log_line: &dyn Fn(&str),
) -> Result<i32, String> {
    init_store(&store).map_err(err)?;
    let ids = resolve_manifests(&store, &query)?;
    for id in ids {
        let stats = push_to_store(&store, &remote, &id, |done, total| {
            eprint!("\rpush {id}: {done}/{total}");
            let _ = std::io::Write::flush(&mut std::io::stderr());
        })
        .map_err(err)?;
        eprintln!();
        log_line(&format!(
            "pushed {} · sent {} · reused {} · {}",
            stats.manifest_id,
            stats.sent_chunks,
            stats.reused_chunks,
            format_bytes(stats.sent_bytes)
        ));
    }
    Ok(EXIT_OK)
}

fn cmd_send(
    query: String,
    store: PathBuf,
    to: String,
    token: Option<String>,
    tls: bool,
    tls_ca: Option<PathBuf>,
    rate: u64,
    log_line: &dyn Fn(&str),
) -> Result<i32, String> {
    let (host, port) = parse_host_port(&to)?;
    init_store(&store).map_err(err)?;
    let ids = resolve_manifests(&store, &query)?;
    let opts = NetOptions {
        token,
        use_tls: tls || tls_ca.is_some(),
        tls_ca,
        rate_limit_bps: rate,
        ..NetOptions::default()
    };
    for id in ids {
        let stats =
            push_over_network_with(&store, &host, port, &id, opts.clone(), |done, total| {
                eprint!("\rsend {id}: {done}/{total}");
                let _ = std::io::Write::flush(&mut std::io::stderr());
            })
            .map_err(err)?;
        eprintln!();
        log_line(&format!(
            "sent {} → {host}:{port} · chunks {} · reused {} · {}",
            stats.manifest_id,
            stats.sent_chunks,
            stats.reused_chunks,
            format_bytes(stats.sent_bytes)
        ));
    }
    Ok(EXIT_OK)
}

fn cmd_serve(
    store: PathBuf,
    bind: String,
    token: Option<String>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    log_line: &dyn Fn(&str),
) -> Result<i32, String> {
    init_store(&store).map_err(err)?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_ctrl = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        stop_ctrl.store(true, Ordering::SeqCst);
    })
    .map_err(|e| format!("ctrlc: {e}"))?;
    let opts = NetOptions {
        token,
        tls_cert,
        tls_key,
        ..NetOptions::default()
    };
    let port = serve_store_with(store.clone(), &bind, Arc::clone(&stop), opts).map_err(err)?;
    log_line(&format!(
        "serving {} on {bind} (port {port}) — Ctrl+C to stop",
        store.display()
    ));
    while !stop.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(200));
    }
    log_line("stopped");
    Ok(EXIT_OK)
}

fn parse_host_port(s: &str) -> Result<(String, u16), String> {
    let s = s.trim();
    if let Some((h, p)) = s.rsplit_once(':') {
        let port: u16 = p.parse().map_err(|_| format!("invalid port in {s}"))?;
        if h.is_empty() {
            return Err("empty host".into());
        }
        return Ok((h.to_string(), port));
    }
    Err(format!("expected host:port, got {s}"))
}
