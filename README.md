# OnDL

OnDL is a **personal, automation-first media ingestion tool**.  
It allows user-initiated media downloads (currently YouTube, with future expansion planned) to be queued, processed locally, and moved into organized storage with preview generation and Discord notifications.

This project is designed for **single-user, intentional workflows**, not public or multi-tenant ingestion.

---

## Features

- Manual job ingestion via `.dljob` files (JSON)
- Local staging for all downloads (network-safe)
- Deterministic output paths
- Preview GIF generation
- Discord webhook notifications
- Graceful fallback when network storage is unavailable
- Configurable via `config.toml`
- OS-agnostic tool resolution (`yt-dlp`, `ffmpeg`, etc.)

---

## High-Level Architecture

```
iOS Shortcut
   ↓ (SSH)
incoming/*.dljob
   ↓
consume.py
   ↓
local staging
   ↓
preview generation
   ↓
Discord webhook
   ↓
final destination (or fallback)
```

All jobs are **explicitly user-requested**.  
There is no background scraping or automatic ingestion.

---

## Job Format

Jobs are JSON files with a `.dljob` extension.

Example:

```json
{
  "url": "https://youtube.com/…",
  "app": "YouTube"
}
```

Jobs are:
- claimed atomically
- processed once
- moved to `done/` or `error/`

---

## Configuration

### Configuration Resolution Order

OnDL resolves configuration in the following order:

1. `config.toml` **next to the executing script**
2. `ONDL_CONFIG` environment variable
3. Defaults only

This allows:
- separate **dev vs prod configs** on the same machine
- stable configs across deploys
- no secrets checked into git

---

### Example Configuration

The repository ships with:

```
config.example.toml
```

To configure OnDL locally:

```bash
cp config.example.toml config.toml
```

⚠️ `config.toml` **should not be committed**.

---

## Tool Resolution

External tools are resolved in this order:

1. Environment variable override  
   (e.g. `ONDL_YTDLP`, `ONDL_FFMPEG`)
2. Explicit path in `config.toml`
3. `PATH` lookup

If a required tool cannot be found, the script fails fast with a clear error.

---

## Running Locally

```bash
python3 consume.py
```

Or explicitly:

```bash
/opt/homebrew/bin/python3 consume.py
```

---

## Deployment Model

OnDL is designed to work well with:

- release directories
- a `current` symlink
- Jenkins or similar CI/CD tooling
- LaunchAgents (macOS)

Configuration is kept **outside release directories** to prevent overwrites during deploys.

---

## Logging & Error Handling

- All subprocess output is captured and logged
- Full tracebacks are written on failure
- Storage fallbacks are surfaced in Discord notifications
- Logs are intended to be prunable (future enhancement)

---

## Project Structure

```
on-dl/
├─ consume.py              # entry point
├─ ingest.py               # job ingestion helper
├─ ondl/
│  ├─ config.py
│  ├─ tools.py
│  ├─ discord.py
│  ├─ jobs.py
│  └─ …
├─ config.example.toml
└─ README.md
```

---

## Design Principles

- **Local first**: never download directly to network storage
- **Explicit intent**: no background or automatic ingestion
- **Failure visibility**: surface issues early and clearly
- **Simple over clever**: clarity > abstraction
- **Configurable but predictable**

---

## Notes on Ethics & Use

OnDL is intended for **personal archival and offline viewing**.  
Users are responsible for complying with the terms of service of any platform they interact with.