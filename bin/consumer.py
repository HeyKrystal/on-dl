#!/opt/homebrew/bin/python3.12
"""OnDL queue consumer.

Consumes JSON job files from INCOMING, downloads media (YouTube via yt-dlp),

renders a short preview GIF, posts to a Discord webhook, and moves jobs to DONE/ERROR.

Job file format (JSON, one object per file):
{
  "url": "https://...",
  "category": "music|sketch|archive|...",
  "app": "YouTube|Instagram|..."
}

Design notes:
- Runtime *state* (queue, logs, previews, archive) lives in an OS-appropriate state directory.
- User *content* (downloads) lives in a user-visible downloads folder by default (~/Downloads/OnDL),
  or a configured path (including mounted network shares).
- Preview artifacts (palette + gif) are deleted after a successful webhook send.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import errno
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# ----------------------------
# Config (TOML)
# ----------------------------

URL_RE = re.compile(r"https?://[^\s\"\'<>]+", re.IGNORECASE)


@dataclass(frozen=True)
class PreviewConfig:
    enabled: bool = True
    duration_seconds: float = 5.0
    fps: int = 12
    width: int = 480
    max_bytes: int = int(7.5 * 1024 * 1024)  # default ~7.5MB safety target


@dataclass(frozen=True)
class QueueConfig:
    max_per_run: int = 5
    job_glob: str = "*.dljob"
    stale_processing_minutes: int = 120
    stale_processing_action: str = "requeue"


@dataclass(frozen=True)
class DiscordConfig:
    webhook_url: str = ""
    username: str = "OnDL"
    avatar_url: str = ""


@dataclass(frozen=True)
class ToolsConfig:
    """Optional explicit tool paths. Leave empty to auto-resolve via PATH."""
    ytdlp: str = ""
    ffmpeg: str = ""
    ffprobe: str = ""
    curl: str = ""


@dataclass(frozen=True)
class OnDLConfig:
    # If empty, use OS-specific default for state_root.
    state_root: str = ""
    # If empty, default to ~/Downloads/OnDL.
    download_root: str = ""
    tools: ToolsConfig = ToolsConfig()
    queue: QueueConfig = QueueConfig()
    preview: PreviewConfig = PreviewConfig()
    discord: DiscordConfig = DiscordConfig()


def default_state_root() -> Path:
    """OS-appropriate per-user state directory."""
    plat = sys.platform
    home = Path.home()

    # macOS
    if plat == "darwin":
        return home / "Library" / "Application Support" / "OnDL"

    # Windows
    if plat.startswith("win"):
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or str(home)
        return Path(base) / "OnDL"

    # Linux and other unix-like
    xdg = os.environ.get("XDG_STATE_HOME") or os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg) / "OnDL"
    return home / ".local" / "share" / "OnDL"


def default_download_root() -> Path:
    """User-visible default downloads directory."""
    return Path.home() / "Downloads" / "OnDL"


def _as_bool(v: object, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return default


def _resolve_path(value: str, *, base_dir: Path) -> Path:
    """Resolve a path from config; relative paths are relative to config file directory."""
    p = Path(os.path.expandvars(os.path.expanduser(value)))
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p

def _env(name: str) -> str:
    """Return an environment variable trimmed; empty string if unset."""
    return os.environ.get(name, "").strip()

def load_config() -> Tuple[OnDLConfig, Path]:
    """Load config.toml located next to this script. Returns (config, config_dir)."""
    script_dir = Path(__file__).resolve().parent
    cfg_path = script_dir / "config.toml"

    cfg = OnDLConfig()

    if not cfg_path.exists():
        return cfg, script_dir

    data = {}
    try:
        with cfg_path.open("rb") as f:
            import tomllib
            data = tomllib.load(f)
    except Exception as e:
        print(f"WARNING: Failed to read config.toml: {e}")
        return cfg, script_dir

    # Top-level sections
    ondldata = data.get("ondl", {}) if isinstance(data, dict) else {}
    queuedata = data.get("queue", {}) if isinstance(data, dict) else {}
    previewdata = data.get("preview", {}) if isinstance(data, dict) else {}
    discorddata = data.get("discord", {}) if isinstance(data, dict) else {}
    toolsdata = data.get("tools", {}) if isinstance(data, dict) else {}

    queue = QueueConfig(
        max_per_run=int(queuedata.get("max_per_run", cfg.queue.max_per_run)),
        job_glob=str(queuedata.get("job_glob", cfg.queue.job_glob)),
        stale_processing_minutes=int(queuedata.get("stale_processing_minutes", cfg.queue.stale_processing_minutes)),
        stale_processing_action=str(queuedata.get("stale_processing_action", cfg.queue.stale_processing_action)),
    )

    preview = PreviewConfig(
        enabled=_as_bool(previewdata.get("enabled", cfg.preview.enabled), cfg.preview.enabled),
        duration_seconds=float(previewdata.get("duration_seconds", cfg.preview.duration_seconds)),
        fps=int(previewdata.get("fps", cfg.preview.fps)),
        width=int(previewdata.get("width", cfg.preview.width)),
        max_bytes=int(previewdata.get("max_bytes", cfg.preview.max_bytes)),
    )

    discord = DiscordConfig(
        webhook_url=(_env("ONDL_DISCORD_WEBHOOK_URL") or str(discorddata.get("webhook_url", cfg.discord.webhook_url)).strip()),
        username=str(discorddata.get("username", cfg.discord.username)).strip(),
        avatar_url=str(discorddata.get("avatar_url", cfg.discord.avatar_url)).strip(),
    )

    tools = ToolsConfig(
        ytdlp=str(toolsdata.get("ytdlp", cfg.tools.ytdlp)).strip(),
        ffmpeg=str(toolsdata.get("ffmpeg", cfg.tools.ffmpeg)).strip(),
        ffprobe=str(toolsdata.get("ffprobe", cfg.tools.ffprobe)).strip(),
        curl=str(toolsdata.get("curl", cfg.tools.curl)).strip(),
    )

    out = OnDLConfig(
        state_root=str(ondldata.get("state_root", cfg.state_root)).strip(),
        download_root=str(ondldata.get("download_root", cfg.download_root)).strip(),
        tools=tools,
        queue=queue,
        preview=preview,
        discord=discord,
    )
    return out, script_dir


# ----------------------------
# Runtime paths (derived from config)
# ----------------------------

CONFIG, CONFIG_DIR = load_config()

STATE_ROOT = _resolve_path(CONFIG.state_root, base_dir=CONFIG_DIR) if CONFIG.state_root else default_state_root()
# Final download destination (may be a network share). If not configured, defaults to ~/Downloads/OnDL.
DOWNLOAD_ROOT = _resolve_path(CONFIG.download_root, base_dir=CONFIG_DIR) if CONFIG.download_root else default_download_root()
# Local staging area for downloads. We always download here first, then move to DOWNLOAD_ROOT.
STAGING_ROOT = STATE_ROOT / "staging-downloads"
# Always-available local fallback if moving to DOWNLOAD_ROOT fails.
FALLBACK_DOWNLOAD_ROOT = default_download_root()

INCOMING = STATE_ROOT / "incoming"
PROCESSING = STATE_ROOT / "processing"
DONE = STATE_ROOT / "done"
ERROR = STATE_ROOT / "error"
LOGS = STATE_ROOT / "logs"
PREVIEWS = STATE_ROOT / "previews"
ARCHIVE = STATE_ROOT / "ytdlp-archive.txt"


# ----------------------------
# Models
# ----------------------------

@dataclass
class Job:
    url: str
    category: str
    app: str
    raw: dict


@dataclass
class Meta:
    id: str
    title: str
    uploader: str
    channel: str
    webpage_url: str
    duration: Optional[float]
    thumbnail: Optional[str]


# ----------------------------
# Utilities
# ----------------------------

def ensure_dirs() -> None:
    # State directories (always local).
    for p in (INCOMING, PROCESSING, DONE, ERROR, LOGS, PREVIEWS, STAGING_ROOT):
        p.mkdir(parents=True, exist_ok=True)

    # Final download root may be a network share; don't fail startup if it's unavailable.
    try:
        DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def reap_stale_processing_jobs() -> int:
    """Move stale jobs out of processing/ on startup.

    Why: If the script crashes or the host reboots mid-job, jobs can sit in processing/
    forever. This startup reaper makes the system self-healing.

    Config:
      - queue.stale_processing_minutes: if <= 0, disabled
      - queue.stale_processing_action: "requeue" (default) or "error"
    """
    minutes = CONFIG.queue.stale_processing_minutes
    if minutes <= 0:
        return 0

    action = (CONFIG.queue.stale_processing_action or "requeue").strip().lower()
    if action not in {"requeue", "error"}:
        raise RuntimeError(f"Invalid queue.stale_processing_action={action!r}; expected 'requeue' or 'error'.")  # noqa: E501

    cutoff = time.time() - (minutes * 60)
    candidates = sorted(PROCESSING.glob(CONFIG.queue.job_glob), key=lambda p: p.stat().st_mtime)

    reaped = 0
    for job_path in candidates:
        try:
            mtime = job_path.stat().st_mtime
        except FileNotFoundError:
            continue  # raced with something else

        if mtime >= cutoff:
            continue  # not stale

        # Decide destination directory.
        dest_dir = INCOMING if action == "requeue" else ERROR
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest = dest_dir / job_path.name
        if dest.exists():
            # Avoid clobbering an existing file.
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            dest = dest_dir / f"{job_path.stem}.reaped-{stamp}{job_path.suffix}"

        try:
            job_path.replace(dest)  # atomic within same filesystem
            reaped += 1
            print(f"Reaped stale processing job -> {dest_dir.name}/: {job_path.name}")
        except Exception as e:
            # Don't crash the whole run just because a reaper move failed.
            print(f"WARN: Failed to reap {job_path.name}: {e}")

    return reaped


def which_or_fail(path: Path, name: str) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing {name} at {path}. Install it (e.g., via Homebrew) or update the path.")


def run(cmd: list[str], *, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=capture, text=True, check=check)


def log_path() -> Path:
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return LOGS / f"consume-{ts}.log"


def write_log_header(fp) -> None:
    fp.write(f"== consume start {datetime.now().isoformat()} ==\n")
    fp.write(f"state_root={STATE_ROOT}\n")
    fp.write(f"download_root={DOWNLOAD_ROOT}\n")
    fp.write(f"job_glob={CONFIG.queue.job_glob} max_per_run={CONFIG.queue.max_per_run}\n")
    fp.flush()


def sanitize_segment(s: str, fallback: str = "unsorted") -> str:
    s = (s or "").strip()
    if not s:
        return fallback
    # Finder/Windows friendly-ish. Keep it simple.
    s = re.sub(r"[\/\\:\*\?\"<>\|]+", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120] if len(s) > 120 else s


def extract_url(text_blob: str) -> Optional[str]:
    m = URL_RE.search(text_blob or "")
    return m.group(0) if m else None


def claim_job(job_path: Path) -> Path:
    claimed = PROCESSING / job_path.name
    job_path.replace(claimed)  # atomic on same filesystem
    return claimed


def finish_job(job_path: Path, ok: bool) -> None:
    target = DONE if ok else ERROR
    target.mkdir(parents=True, exist_ok=True)
    job_path.replace(target / job_path.name)


def parse_job_file(path: Path) -> Job:
    blob = path.read_text(encoding="utf-8", errors="replace").strip()

    # JSON job is preferred; fallback to url-only.
    if blob.startswith("{"):
        data = json.loads(blob)
        url = str(data.get("url") or "").strip()
        category = str(data.get("category") or "unsorted").strip()
        app = str(data.get("app") or "YouTube").strip()
    else:
        url = extract_url(blob) or (blob.splitlines()[0].strip() if blob else "")
        category = "unsorted"
        app = "YouTube"
        data = {"url": url, "category": category, "app": app}

    if not url.lower().startswith("http"):
        raise RuntimeError(f"Job does not contain a URL. First 200 chars: {blob[:200]!r}")

    return Job(url=url, category=category, app=app, raw=data)


def resolve_tool(exe_name: str, *, env_var: str | None = None, config_value: str = "", config_key: str = "") -> Path:
    """Resolve an external tool path.
    Resolution order:
      1) environment variable (if provided)
      2) config value (if provided)
      3) PATH (shutil.which)
    Raises RuntimeError with a clear message if the tool can't be found.
    """
    override = ""
    if env_var:
        override = os.environ.get(env_var, "").strip()
    if not override and config_value:
        override = str(config_value).strip()

    if override:
        p = Path(os.path.expandvars(os.path.expanduser(override)))
        if p.exists():
            return p
        hint = ""
        if env_var or config_key:
            parts = []
            if env_var:
                parts.append(env_var)
            if config_key:
                parts.append(f"[tools].{config_key}")
            hint = f" (set {' or '.join(parts)})"
        raise RuntimeError(f"Configured tool path for '{exe_name}' not found: {p}{hint}")

    found = shutil.which(exe_name)
    if found:
        return Path(found)

    parts = []
    if env_var:
        parts.append(env_var)
    if config_key:
        parts.append(f"[tools].{config_key}")
    hint = f" Set {' or '.join(parts)}." if parts else ""
    raise RuntimeError(f"Required tool '{exe_name}' not found on PATH.{hint}")


# Resolve required tools after config load
YTDLP = resolve_tool("yt-dlp", env_var="ONDL_YTDLP", config_value=CONFIG.tools.ytdlp, config_key="ytdlp")
FFMPEG = resolve_tool("ffmpeg", env_var="ONDL_FFMPEG", config_value=CONFIG.tools.ffmpeg, config_key="ffmpeg")
FFPROBE = resolve_tool("ffprobe", env_var="ONDL_FFPROBE", config_value=CONFIG.tools.ffprobe, config_key="ffprobe")
CURL = resolve_tool("curl", env_var="ONDL_CURL", config_value=CONFIG.tools.curl, config_key="curl")

# ----------------------------
# YouTube (yt-dlp)
# ----------------------------

def get_youtube_meta(url: str) -> Meta:
    cp = run([str(YTDLP), "-J", "--no-playlist", url], capture=True, check=True)
    data = json.loads(cp.stdout)
    return Meta(
        id=str(data.get("id") or ""),
        title=str(data.get("title") or "Untitled"),
        uploader=str(data.get("uploader") or ""),
        channel=str(data.get("channel") or data.get("uploader") or ""),
        webpage_url=str(data.get("webpage_url") or url),
        duration=float(data["duration"]) if data.get("duration") is not None else None,
        thumbnail=str(data.get("thumbnail")) if data.get("thumbnail") else None,
    )


def download_youtube(url: str, category: str) -> Path:
    """Download a YouTube URL to the local staging area and return the local filepath."""
    # staging-downloads/<category>/<uploader>/<title> [id].ext
    cat_dir = STAGING_ROOT / sanitize_segment(category)
    cat_dir.mkdir(parents=True, exist_ok=True)

    outtmpl = str(cat_dir / "%(uploader)s" / "%(uploader)s - %(title)s.%(ext)s")

    cmd = [
        str(YTDLP),
        "--no-playlist",
        "--ignore-errors",
        "--continue",
        "--write-thumbnail",
        "--convert-thumbnails", "jpg",
        #"--download-archive", str(ARCHIVE),
        "--ffmpeg-location", str(FFMPEG),
        "--print", "after_move:filepath",
        "--windows-filenames",
        "-o", outtmpl,
        url,
    ]

    try:
        cp = run(cmd, capture=True, check=True)
    except subprocess.CalledProcessError as e:
        # Include yt-dlp's stderr/stdout in the exception message for the log.
        msg = [f"yt-dlp failed (exit={e.returncode})", f"Command: {cmd}"]
        if e.stdout:
            msg.append(f"stdout:\n{e.stdout}")
        if e.stderr:
            msg.append(f"stderr:\n{e.stderr}")
        raise RuntimeError("\n".join(msg)) from e

    lines = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        p = Path(ln)
        if p.exists():
            tp = p.with_suffix(".jpg")
            if tp.exists():
                plextp = tp.with_name(f"{tp.stem}-poster{tp.suffix}")
                tp.rename(plextp)
            return p

    raise RuntimeError(f"Download finished but could not locate output file. yt-dlp output:\n{cp.stdout}")




def move_merge(src: Path, dest: Path) -> None:
    """Move a file or directory into dest, merging directories recursively.

    This is like a very small 'rsync --remove-source-files' for our use case.
    Existing files at the destination are overwritten.
    """
    if src.is_dir():
        dest.mkdir(parents=True, exist_ok=True)
        for child in src.iterdir():
            move_merge(child, dest / child.name)
        # Try to remove empty source directory
        try:
            src.rmdir()
        except OSError:
            pass
        return

    # src is a file
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    shutil.move(str(src), str(dest))


def finalize_download(local_path: Path, *, category: str, uploader: Optional[str] = None) -> tuple[Path, bool]:
    """Move a locally-staged download into the configured download root.

    If moving to DOWNLOAD_ROOT fails (e.g., share not mounted), moves into FALLBACK_DOWNLOAD_ROOT instead.
    Returns the final path.
    """
    # Preserve relative structure from staging: <category>/<uploader>/<filename>
    try:
        rel = local_path.relative_to(STAGING_ROOT)
    except Exception:
        # If for some reason the file isn't under staging, just place it under category.
        rel = Path(sanitize_segment(category)) / local_path.name

    primary_dest = DOWNLOAD_ROOT / rel
    fallback_used = False
    try:
        move_merge(local_path.parent, primary_dest.parent)
        return primary_dest, fallback_used
    except Exception as e:
        # Fallback to local Downloads/OnDL
        fallback_used = True
        fallback_dest = FALLBACK_DOWNLOAD_ROOT / rel
        move_merge(local_path.parent, fallback_dest.parent)
        print(f"WARNING: Move to configured download_root failed ({DOWNLOAD_ROOT}). "
              f"Fell back to {fallback_dest}. Reason: {e}", file=sys.stderr)
        return fallback_dest, fallback_used


def ffprobe_duration(filepath: Path) -> Optional[float]:
    try:
        cp = run(
            [str(FFPROBE), "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(filepath)],
            capture=True,
            check=True,
        )
        val = (cp.stdout or "").strip()
        return float(val) if val else None
    except Exception:
        return None

def pick_preview_start(duration: Optional[float], clip_len: float) -> float:
    # Early-but-not-the-start:
    # start = min(30, max(12, dur*0.10)), clamped for very short videos.
    if not duration or duration <= 0:
        return 12.0
    start = min(30.0, max(12.0, duration * 0.10))
    if duration < (clip_len + 2.0):
        return max(0.0, min(2.0, duration * 0.2))
    if start + clip_len > duration:
        start = max(0.0, duration - clip_len - 1.0)
    return round(start, 2)


def make_gif(video_path: Path, start: float, base_name: str, preview: PreviewConfig) -> Path:
    fps = preview.fps
    width = preview.width
    max_bytes = preview.max_bytes
    clip_len = preview.duration_seconds

    palette = PREVIEWS / f"{base_name}-palette.png"

    try:
        while True:
            gif_path = PREVIEWS / f"{base_name}-w{width}-fps{fps}.gif"

            # Palette gen
            run([
                str(FFMPEG), "-v", "error",
                "-ss", str(start), "-t", str(clip_len),
                "-i", str(video_path),
                "-vf", f"fps={fps},scale={width}:-1:flags=lanczos,palettegen",
                "-y", str(palette),
            ], capture=True, check=True)

            # Palette apply
            run([
                str(FFMPEG), "-v", "error",
                "-ss", str(start), "-t", str(clip_len),
                "-i", str(video_path),
                "-i", str(palette),
                "-lavfi", f"fps={fps},scale={width}:-1:flags=lanczos [x]; [x][1:v] paletteuse=dither=bayer:bayer_scale=3",
                "-y", str(gif_path),
            ], capture=True, check=True)

            size = gif_path.stat().st_size
            if size <= max_bytes:
                return gif_path

            # Reduce quality iteratively (width first, then fps).
            if width > 320:
                width = 360 if width >= 480 else max(320, width - 60)
            elif fps > 10:
                fps -= 2
            else:
                return gif_path

    finally:
        # Always delete palette (we never need to keep it).
        try:
            if palette.exists():
                palette.unlink()
        except Exception:
            pass


# ----------------------------
# Discord webhook
# ----------------------------

def discord_post(meta: Meta, preview_gif: Optional[Path], start: float, video_path: Path, discord: DiscordConfig, save_fallback: bool) -> bool:
    if not discord.webhook_url:
        print("Discord webhook_url not set; skipping webhook.")
        return False

    gif_name = "preview.gif"

    def fmt_dur(seconds: Optional[float]) -> str:
        if not seconds:
            return "unknown"
        s = int(seconds)
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"

    size_mb = video_path.stat().st_size / (1024 * 1024)

    embed = {
        "title": meta.title,
        "url": meta.webpage_url,
        "description": f"Downloaded and queued preview (starts at ~{start:.2f}s).",
        "fields": [
            {"name": "Channel", "value": meta.channel or meta.uploader or "unknown", "inline": True},
            {"name": "Duration", "value": fmt_dur(meta.duration), "inline": True},
            {"name": "File Size", "value": f"{size_mb:.2f} MB", "inline": True},
            {"name": "Saved To", "value": str(video_path), "inline": False},
        ],
    }

    if save_fallback:
        embed["fields"].append({
            "name": "⚠️ Storage Fallback Used",
            "value": "Download root unavailable; file saved locally. Check logs for details.",
            "inline": False,
        })

    if meta.thumbnail:
        embed["thumbnail"] = {"url": meta.thumbnail}

    if preview_gif is not None:
        embed["image"] = {"url": f"attachment://{gif_name}"}

    payload = {
        "content": "📥 **OnDL download complete**",
        "embeds": [embed],
    }

    if discord.username:
        payload["username"] = discord.username
    if discord.avatar_url:
        payload["avatar_url"] = discord.avatar_url

    args = [
        str(CURL), "-sS", "-X", "POST", discord.webhook_url,
        "-F", f"payload_json={json.dumps(payload)}",
    ]
    if preview_gif is not None:
        args += ["-F", f"file=@{str(preview_gif)};filename={gif_name}"]

    cp = run(args, capture=True, check=False)

    if cp.returncode == 0:
        return True

    err = (cp.stderr or "").strip()
    if err:
        print(f"Webhook curl error: {err}")
    return False


# ----------------------------
# App dispatch
# ----------------------------

def download_for_job(job: Job) -> Tuple[Meta, Path]:
    app = (job.app or "YouTube").strip().lower()
    if app in ("youtube", "yt"):
        meta = get_youtube_meta(job.url)
        video_path = download_youtube(job.url, job.category)
        return meta, video_path

    if app in ("instagram", "ig"):
        # Placeholder for future implementation.
        raise NotImplementedError("Instagram downloading not implemented yet.")

    raise RuntimeError(f"Unsupported app '{job.app}'.")


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ensure_dirs()
    
    reaped = reap_stale_processing_jobs()
    if reaped:
        print(f"Reaper moved {reaped} stale job(s) out of processing/.")

    which_or_fail(YTDLP, "yt-dlp")
    which_or_fail(FFMPEG, "ffmpeg")
    which_or_fail(FFPROBE, "ffprobe")
    which_or_fail(CURL, "curl")

    # Preflight: local staging must be writable. Final download root may be a network share.
    try:
        test_dir = STAGING_ROOT
        test_dir.mkdir(parents=True, exist_ok=True)
        probe = test_dir / ".ondl_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as e:
        raise RuntimeError(f"Staging root not writable: {STAGING_ROOT} ({e})")

    # Best-effort check for configured download_root (don't fail startup if unavailable).
    try:
        test_dir = DOWNLOAD_ROOT
        test_dir.mkdir(parents=True, exist_ok=True)
        probe = test_dir / ".ondl_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as e:
        print(f"WARNING: download_root not writable right now ({DOWNLOAD_ROOT}): {e}", file=sys.stderr)


    lp = log_path()
    with lp.open("a", encoding="utf-8") as logfp:
        class Tee:
            def __init__(self, *streams):
                self.streams = streams

            def write(self, s):
                for st in self.streams:
                    try:
                        st.write(s)
                    except (ValueError, BrokenPipeError):
                        pass
                for st in self.streams:
                    try:
                        st.flush()
                    except (ValueError, BrokenPipeError):
                        pass

            def flush(self):
                for st in self.streams:
                    try:
                        st.flush()
                    except (ValueError, BrokenPipeError):
                        pass

        orig_out, orig_err = sys.stdout, sys.stderr
        sys.stdout = Tee(sys.__stdout__, logfp)  # type: ignore
        sys.stderr = Tee(sys.__stderr__, logfp)  # type: ignore

        try:
            write_log_header(logfp)

            jobs = sorted(INCOMING.glob(CONFIG.queue.job_glob), key=lambda p: p.stat().st_mtime)
            processed = 0

            for job_path in jobs:
                if processed >= CONFIG.queue.max_per_run:
                    print(f"Reached max_per_run={CONFIG.queue.max_per_run}")
                    break

                print("\n---")
                print(f"Job incoming: {job_path.name}")

                ok = False
                claimed: Optional[Path] = None
                preview_path: Optional[Path] = None

                try:
                    claimed = claim_job(job_path)
                    job = parse_job_file(claimed)
                    print(f"Parsed job: app={job.app} category={job.category} url={job.url}")

                    meta, staged_path = download_for_job(job)
                    print(f"Downloaded (staging): {staged_path}")

                    # Preview (optional) - generated from the local staged file for reliability.
                    if CONFIG.preview.enabled:
                        dur = meta.duration or ffprobe_duration(staged_path)
                        start = pick_preview_start(dur, CONFIG.preview.duration_seconds)
                        base_name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{meta.id or 'noid'}"
                        preview_path = make_gif(staged_path, start, base_name, CONFIG.preview)
                        print(f"Preview GIF: {preview_path} ({preview_path.stat().st_size/1024:.1f} KB)")
                    else:
                        start = 0.0


                    # Move staged download to final destination (network share) AFTER we have the preview.
                    video_path, fallback_used = finalize_download(staged_path, category=job.category, uploader=meta.channel)
                    print(f"Finalized download: {video_path}")

                    # Webhook (optional)
                    sent = discord_post(meta, preview_path, start, video_path, CONFIG.discord, fallback_used)
                    if sent:
                        print("Webhook: sent")
                        # Cleanup preview after successful send (you said you don't need it).
                        if preview_path and preview_path.exists():
                            try:
                                preview_path.unlink()
                                preview_path = None
                            except Exception:
                                pass
                    else:
                        print("Webhook: not sent (or disabled). Keeping preview for debugging.")

                    ok = True

                except Exception as e:
                    print(f"ERROR: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc(file=sys.stderr)
                    ok = False

                finally:
                    if claimed:
                        finish_job(claimed, ok)

                processed += 1

            print(f"== consume end {datetime.now().isoformat()} processed={processed} ==")

        finally:
            sys.stdout, sys.stderr = orig_out, orig_err

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
