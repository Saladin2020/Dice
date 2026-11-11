# Dice portable

CDC chunk store with compression, dedup sync, nested folders, CLI + desktop UI.

## Files

- `dice.exe` — cross-platform CLI (this build: Windows)
- `dice-desktop.exe` — GUI (Windows)

## Quick start (GUI)

1. Run `dice-desktop.exe`
2. Target → select files/folders → **Convert → Store**
3. Destination → **Restore**
4. Transfer: **Serve** on one machine, **Send** from another (`host:7788`)

Settings: `Dice → Settings…` (chunk sizes, workers, excludes, auth token, TLS, send rate).

## Quick start (CLI)

```bat
dice init --store .dice-store
dice put .\my-folder --store .dice-store
dice list --store .dice-store
dice stats --store .dice-store
dice restore all --store .dice-store --out .\out
dice verify --store .dice-store
dice serve --store .dice-store --bind 0.0.0.0:7788 --token secret
dice send all --store .dice-store --to 192.168.1.10:7788 --token secret
```

Resume interrupted convert:

```bat
dice put --resume --store .dice-store
```

TLS (optional):

```bat
dice serve --store .dice-store --tls-cert cert.pem --tls-key key.pem --token secret
dice send all --store .dice-store --to host:7788 --tls --tls-ca cert.pem --token secret --rate 10485760
```

## Exit codes (CLI)

| Code | Meaning |
|------|---------|
| 0 | OK |
| 1 | Error |
| 2 | Verify found issues |
| 3 | Partial failure |

## Notes

- Default TCP port: **7788**
- Store lock is cross-process (CLI + GUI safe)
- Nested folder paths are preserved on restore
- Empty folders are skipped
