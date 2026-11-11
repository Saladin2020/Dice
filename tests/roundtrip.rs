use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dice_desktop::assemble::{assemble_file, restore_output_path};
use dice_desktop::chunker::ChunkerOptions;
use dice_desktop::ingest::ingest_file;
use dice_desktop::store::list_manifests;
use dice_desktop::transport::{push_over_network, push_to_store, serve_store};

fn temp_dir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("dice-desktop-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn roundtrip_dedup_and_sync() {
    let base = temp_dir("rt");
    let store_a = base.join("a");
    let store_b = base.join("b");
    let store_c = base.join("c");

    let chunker = ChunkerOptions {
        min_size: 64,
        avg_size: 256,
        max_size: 1024,
    };

    let mut original = vec![0u8; 8192];
    for (i, b) in original.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let file1 = base.join("v1.bin");
    fs::write(&file1, &original).unwrap();

    let put1 = ingest_file(&store_a, &file1, None, chunker, 1, |_, _| {}).unwrap();
    assert!(put1.manifest.chunks.len() >= 2);

    let out1 = base.join("out1.bin");
    assemble_file(&store_a, &put1.manifest.id, &out1, |_, _| {}).unwrap();
    assert_eq!(fs::read(&out1).unwrap(), original);

    let mut edited = original.clone();
    edited[4000] = edited[4000].wrapping_add(1);
    let file2 = base.join("v2.bin");
    fs::write(&file2, &edited).unwrap();
    let put2 = ingest_file(&store_a, &file2, None, chunker, 1, |_, _| {}).unwrap();
    assert!(put2.reused_chunks > 0);
    assert!(put2.new_chunks > 0);

    let sync1 = push_to_store(&store_a, &store_b, &put1.manifest.id, |_, _| {}).unwrap();
    assert!(sync1.sent_chunks > 0);
    let sync2 = push_to_store(&store_a, &store_b, &put2.manifest.id, |_, _| {}).unwrap();
    assert!(sync2.reused_chunks > 0);

    let out2 = base.join("out2.bin");
    assemble_file(&store_b, &put2.manifest.id, &out2, |_, _| {}).unwrap();
    assert_eq!(fs::read(&out2).unwrap(), edited);

    // free port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let stop = Arc::new(AtomicBool::new(false));
    let served = serve_store(store_c.clone(), &format!("127.0.0.1:{port}"), stop.clone()).unwrap();
    assert_eq!(served, port);

    let net1 = push_over_network(&store_a, "127.0.0.1", port, &put1.manifest.id, |_, _| {}).unwrap();
    assert!(net1.sent_chunks > 0);
    assert_eq!(list_manifests(&store_c).unwrap().len(), 1);

    let net2 = push_over_network(&store_a, "127.0.0.1", port, &put2.manifest.id, |_, _| {}).unwrap();
    assert!(net2.reused_chunks > 0);

    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn nested_folder_expand_ingest_restore() {
    let base = temp_dir("nested");
    let src = base.join("src");
    let store = base.join("store");
    let dest = base.join("dest");
    fs::create_dir_all(src.join("docs").join("2024")).unwrap();
    fs::create_dir_all(src.join("empty_dir")).unwrap();
    fs::write(src.join("root.txt"), b"root").unwrap();
    fs::write(src.join("docs").join("a.txt"), b"aaa").unwrap();
    fs::write(src.join("docs").join("2024").join("b.txt"), b"bbb").unwrap();
    fs::create_dir_all(&dest).unwrap();

    let selected = dice_desktop::fs_browser::expand_selection_filtered(
        &src,
        [src.join("docs"), src.join("root.txt")],
        &dice_desktop::exclude::ExcludeSet::from_defaults(),
    )
    .unwrap();
    let rels: Vec<_> = selected.iter().map(|s| s.relative_path.as_str()).collect();
    assert_eq!(rels, vec!["docs/2024/b.txt", "docs/a.txt", "root.txt"]);

    let chunker = ChunkerOptions {
        min_size: 64,
        avg_size: 256,
        max_size: 1024,
    };
    for f in &selected {
        let put = ingest_file(
            &store,
            &f.path,
            Some(&f.relative_path),
            chunker,
            1,
            |_, _| {},
        )
        .unwrap();
        assert_eq!(put.manifest.logical_path(), f.relative_path);
        let out = restore_output_path(&dest, &put.manifest).unwrap();
        assemble_file(&store, &put.manifest.id, &out, |_, _| {}).unwrap();
        assert_eq!(fs::read(&out).unwrap(), fs::read(&f.path).unwrap());
    }

    assert!(dest.join("docs").join("2024").join("b.txt").is_file());
    assert!(dest.join("docs").join("a.txt").is_file());
    assert!(dest.join("root.txt").is_file());
    // Empty folders are not reconstructed (no files under them).
    assert!(!dest.join("empty_dir").exists());

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn legacy_manifest_without_relative_path_still_loads() {
    let base = temp_dir("legacy");
    let store = base.join("store");
    fs::create_dir_all(store.join("manifests")).unwrap();
    fs::create_dir_all(store.join("chunks")).unwrap();
    fs::write(store.join("refs.json"), "{}\n").unwrap();

    let raw = r#"{
  "version": 1,
  "id": "abcd1234-legacy",
  "name": "flat.bin",
  "size": 0,
  "hash": "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
  "codec": "zstd",
  "level": 1,
  "chunker": { "algo": "fastcdc", "minSize": 64, "avgSize": 128, "maxSize": 256 },
  "chunks": [],
  "createdAt": "2026-01-01T00:00:00Z"
}"#;
    fs::write(store.join("manifests").join("abcd1234-legacy.json"), raw).unwrap();
    let list = list_manifests(&store).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].logical_path(), "flat.bin");
    assert!(list[0].relative_path.is_empty());
    let _ = fs::remove_dir_all(&base);
}
