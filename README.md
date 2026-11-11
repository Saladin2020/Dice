<p align="center">
  <img src="assets/icon.png" alt="Dice" width="96" height="96">
</p>

<h1 align="center">Dice</h1>

<p align="center">
  <strong>Chunk once. Dedup everywhere. Restore bit-exact.</strong>
</p>

<p align="center">
  A fast content-defined chunk store for large files and folder trees —<br>
  with a native desktop UI and a cross-platform CLI.
</p>

<p align="center">
  <a href="#why-dice">Why</a> ·
  <a href="#how-it-works">How it works</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#cli">CLI</a> ·
  <a href="#build">Build</a>
</p>

---

## Why Dice

Copying big folders is wasteful. Most bytes barely change — but tools still push everything again.

Dice splits data with **FastCDC**, fingerprints chunks with **BLAKE3**, compresses with **zstd**, and keeps a content-addressed store. Identical chunks are stored once. Sync only sends what’s missing. Restore rebuilds the original bytes — **bit-exact**.

| | |
|---|---|
| **Speed-first** | Tunable chunk sizes, worker pool, mmap ingest |
| **Dedup that compounds** | Shared chunks across files and machines |
| **Folder trees** | Nested paths preserved on restore |
| **Two faces** | Desktop GUI for day-to-day · CLI for servers & scripts |
| **Safe transfer** | Optional auth token, TLS, bandwidth cap |
| **Resilient** | Resume interrupted converts · verify · GC |

---

## How it works

```text
  files / folders
        │
        ▼
 ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
 │   FastCDC    │ ──▶ │ BLAKE3 + zstd│ ──▶ │  Dice store  │
 │  chunking    │     │  fingerprint │     │ chunks +     │
 └──────────────┘     └──────────────┘     │ manifests    │
                                           └──────┬───────┘
                    restore ◀─────────────────────┤
                    serve / send ◀────────────────┘
```

1. **Convert** — walk trees, skip junk (`.git`, `node_modules`, …), chunk, compress, store  
2. **Dedup** — same content → same chunk id → one copy on disk  
3. **Transfer** — push only missing chunks to another store  
4. **Restore** — reassemble files and folders exactly  

Default TCP port: **7788**

---

## Quick start

### Desktop

```bat
cargo run --release --bin dice-desktop --features gui
```

1. Pick **Target** files or folders → **Convert → Store**  
2. Open **Destination** → **Restore**  
3. On another machine: **Serve**, then **Send** to `host:7788`  

Tune chunk sizes, workers, excludes, auth, TLS, and send rate in **Settings**.

### CLI in 30 seconds

```bat
dice init --store .dice-store
dice put .\my-folder --store .dice-store
dice stats --store .dice-store
dice restore all --store .dice-store --out .\out
```

---

## CLI

```bat
# Inventory & integrity
dice list --store .dice-store
dice stats --store .dice-store
dice verify --store .dice-store

# Cleanup
dice rm <query> --store .dice-store
dice gc --store .dice-store

# Resume a crashed convert
dice put --resume --store .dice-store

# Network sync
dice serve --store .dice-store --bind 0.0.0.0:7788 --token secret
dice send all --store .dice-store --to 192.168.1.10:7788 --token secret

# TLS + rate limit (10 MiB/s)
dice serve --store .dice-store --tls-cert cert.pem --tls-key key.pem --token secret
dice send all --store .dice-store --to host:7788 --tls --tls-ca cert.pem --token secret --rate 10485760
```

### Exit codes

| Code | Meaning |
|:----:|---------|
| `0` | OK |
| `1` | Error |
| `2` | Verify found issues |
| `3` | Partial failure |

---

## Build

Requires a recent [Rust](https://rustup.rs) toolchain.

```bat
# CLI only (servers / CI)
cargo build --release --bin dice --no-default-features

# Desktop + CLI
cargo build --release --bin dice
cargo build --release --bin dice-desktop --features gui
```

| Binary | Output |
|--------|--------|
| CLI | `target/release/dice` |
| GUI | `target/release/dice-desktop` |

---

## Under the hood

- **FastCDC** — content-defined chunking (min / avg / max tunable)  
- **BLAKE3** — chunk & file hashing  
- **zstd** — per-chunk compression  
- **Refcounted store** — safe delete + GC  
- **Journaled convert** — resume mid-job  
- **Cross-process lock** — CLI and GUI can share a store  
- **rustls** — optional TLS for transfer  

---

## License

Source is available in this repository. Add a license file if you want to clarify redistribution terms.
