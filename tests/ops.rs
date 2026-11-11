use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dice_desktop::chunker::ChunkerOptions;
use dice_desktop::gc::{delete_manifest, gc_store};
use dice_desktop::ingest::ingest_file;
use dice_desktop::job::{ConvertJob, JobFile, JobFileStatus};
use dice_desktop::store::{init_store, list_manifests};
use dice_desktop::transport::{push_over_network_with, serve_store_with, NetOptions};

fn temp_dir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "dice-ops-{name}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn small_chunker() -> ChunkerOptions {
    ChunkerOptions {
        min_size: 64,
        avg_size: 256,
        max_size: 1024,
    }
}

#[test]
fn delete_and_gc_removes_unreferenced_chunks() {
    let base = temp_dir("gc");
    let store = base.join("store");
    let file = base.join("a.bin");
    fs::write(&file, vec![7u8; 2000]).unwrap();

    let put = ingest_file(&store, &file, Some("a.bin"), small_chunker(), 1, |_, _| {}).unwrap();
    assert!(!put.skipped);
    assert!(!put.manifest.chunks.is_empty());

    let before = list_manifests(&store).unwrap().len();
    assert_eq!(before, 1);

    let del = delete_manifest(&store, &put.manifest.id).unwrap();
    assert!(del.deleted_chunks > 0);
    assert!(list_manifests(&store).unwrap().is_empty());

    let gc = gc_store(&store).unwrap();
    assert_eq!(gc.kept_chunks, 0);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn resume_journal_roundtrip() {
    let base = temp_dir("resume");
    let store = base.join("store");
    init_store(&store).unwrap();
    let f1 = base.join("one.txt");
    let f2 = base.join("two.txt");
    fs::write(&f1, b"aaa").unwrap();
    fs::write(&f2, b"bbb").unwrap();

    let mut job = ConvertJob {
        version: 1,
        store: store.display().to_string(),
        base: base.display().to_string(),
        zstd_level: 1,
        min_size: 64,
        avg_size: 256,
        max_size: 1024,
        workers: 2,
        files: vec![
            JobFile {
                absolute: f1.display().to_string(),
                relative_path: "one.txt".into(),
                size: 3,
                status: JobFileStatus::Done,
                error: String::new(),
                manifest_id: "x".into(),
            },
            JobFile {
                absolute: f2.display().to_string(),
                relative_path: "two.txt".into(),
                size: 3,
                status: JobFileStatus::Pending,
                error: String::new(),
                manifest_id: String::new(),
            },
        ],
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    let path = ConvertJob::journal_path(&store);
    job.save(&path).unwrap();
    let loaded = ConvertJob::load(&path).unwrap();
    assert_eq!(loaded.pending_count(), 1);

    let pending = loaded
        .files
        .into_iter()
        .filter(|f| f.status == JobFileStatus::Pending)
        .collect::<Vec<_>>();
    assert_eq!(pending.len(), 1);
    let put = ingest_file(
        &store,
        &pending[0].absolute,
        Some(&pending[0].relative_path),
        small_chunker(),
        1,
        |_, _| {},
    )
    .unwrap();
    assert!(!put.skipped);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn token_auth_rejects_bad_token() {
    let base = temp_dir("auth");
    let store_a = base.join("a");
    let store_b = base.join("b");
    let file = base.join("x.bin");
    fs::write(&file, vec![1u8; 1500]).unwrap();
    let put = ingest_file(&store_a, &file, None, small_chunker(), 1, |_, _| {}).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let stop = Arc::new(AtomicBool::new(false));
    let opts = NetOptions {
        token: Some("secret".into()),
        ..NetOptions::default()
    };
    serve_store_with(
        store_b.clone(),
        &format!("127.0.0.1:{port}"),
        stop.clone(),
        opts,
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));

    let bad = push_over_network_with(
        &store_a,
        "127.0.0.1",
        port,
        &put.manifest.id,
        NetOptions {
            token: Some("wrong".into()),
            ..NetOptions::default()
        },
        |_, _| {},
    );
    assert!(bad.is_err(), "bad token should fail");

    let good = push_over_network_with(
        &store_a,
        "127.0.0.1",
        port,
        &put.manifest.id,
        NetOptions {
            token: Some("secret".into()),
            ..NetOptions::default()
        },
        |_, _| {},
    );
    assert!(good.is_ok(), "good token should work: {good:?}");

    stop.store(true, Ordering::SeqCst);
    let _ = fs::remove_dir_all(&base);
}
