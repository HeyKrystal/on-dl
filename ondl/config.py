from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import tomllib


def default_state_root() -> Path:
    """OS-appropriate state dir (queue, logs, previews, archive)."""
    home = Path.home()
    if sys.platform == "darwin":
        return home / "Library" / "Application Support" / "OnDL"
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA")
        return Path(local) / "OnDL" if local else home / "AppData" / "Local" / "OnDL"
    xdg = os.environ.get("XDG_STATE_HOME")
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
    if isinstance(v, (int, float)):
        return bool(v)
    return default


def _resolve_path(p: str, *, base_dir: Path) -> Path:
    # Expand env vars and ~. If relative, treat relative to config file dir.
    expanded = os.path.expandvars(os.path.expanduser(p))
    path = Path(expanded)
    return path if path.is_absolute() else (base_dir / path)


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


@dataclass(frozen=True)
class PreviewConfig:
    gif_seconds: float = 4.0
    gif_fps: int = 12
    gif_width: int = 480
    gif_max_bytes: int = 7_864_320  # Discord upload comfort zone


@dataclass(frozen=True)
class QueueConfig:
    max_per_run: int = 5
    job_glob: str = "*.dljob"
    stale_processing_minutes: int = 0  # 0 disables; set to e.g. 120
    stale_processing_action: str = "requeue"  # or "error"


@dataclass(frozen=True)
class DiscordConfig:
    webhook_url: str = ""
    username: str = ""
    avatar_url: str = ""
    author_icon_url: str = ""


@dataclass(frozen=True)
class ToolsConfig:
    ytdlp: str = ""
    ffmpeg: str = ""
    ffprobe: str = ""
    curl: str = ""


@dataclass(frozen=True)
class OnDLConfig:
    state_root: str = ""
    download_root: str = ""
    tools: ToolsConfig = ToolsConfig()
    queue: QueueConfig = QueueConfig()
    preview: PreviewConfig = PreviewConfig()
    discord: DiscordConfig = DiscordConfig()


def load_config(script_path: Path) -> Tuple[OnDLConfig, Path]:
    """Load config.toml next to the entry script, or via ONDL_CONFIG."""
    script_dir = script_path.resolve().parent

    # Defaults
    cfg = OnDLConfig()
    data: dict = {}
    cfg_dir = script_dir  # where the config was loaded from

    # Prefer local config.toml next to the executing script
    local_cfg = script_dir / "config.toml"
    if local_cfg.exists():
        data = tomllib.loads(local_cfg.read_text(encoding="utf-8"))
        cfg_dir = local_cfg.parent
    else:
        # Fall back to ONDL_CONFIG env var
        env_cfg = os.environ.get("ONDL_CONFIG", "").strip()
        if env_cfg:
            env_path = Path(os.path.expandvars(os.path.expanduser(env_cfg)))
            if not env_path.exists():
                raise RuntimeError(f"ONDL_CONFIG points to missing file: {env_path}")
            data = tomllib.loads(env_path.read_text(encoding="utf-8"))
            cfg_dir = env_path.parent

    ondl = data.get("ondl", {}) or {}
    tools_tbl = data.get("tools", {}) or {}
    queue_tbl = data.get("queue", {}) or {}
    preview_tbl = data.get("preview", {}) or {}
    discord_tbl = data.get("discord", {}) or {}

    tools = ToolsConfig(
        ytdlp=str(tools_tbl.get("ytdlp", "")).strip(),
        ffmpeg=str(tools_tbl.get("ffmpeg", "")).strip(),
        ffprobe=str(tools_tbl.get("ffprobe", "")).strip(),
        curl=str(tools_tbl.get("curl", "")).strip(),
    )

    queue = QueueConfig(
        max_per_run=int(queue_tbl.get("max_per_run", cfg.queue.max_per_run)),
        job_glob=str(queue_tbl.get("job_glob", cfg.queue.job_glob)).strip() or cfg.queue.job_glob,
        stale_processing_minutes=int(queue_tbl.get("stale_processing_minutes", cfg.queue.stale_processing_minutes)),
        stale_processing_action=str(queue_tbl.get("stale_processing_action", cfg.queue.stale_processing_action)).strip() or cfg.queue.stale_processing_action,
    )

    preview = PreviewConfig(
        gif_seconds=float(preview_tbl.get("gif_seconds", cfg.preview.gif_seconds)),
        gif_fps=int(preview_tbl.get("gif_fps", cfg.preview.gif_fps)),
        gif_width=int(preview_tbl.get("gif_width", cfg.preview.gif_width)),
        gif_max_bytes=int(preview_tbl.get("gif_max_bytes", cfg.preview.gif_max_bytes)),
    )

    # Discord: env override first, then TOML
    discord = DiscordConfig(
    webhook_url=_env("ONDL_DISCORD_WEBHOOK_URL") or str(discord_tbl.get("webhook_url", "")).strip(),
    username=str(discord_tbl.get("username", "")).strip(),
    avatar_url=str(discord_tbl.get("avatar_url", "")).strip(),
    author_icon_url=str(discord_tbl.get("author_icon_url", "")).strip(),
)

    out = OnDLConfig(
        state_root=str(ondl.get("state_root", cfg.state_root)).strip(),
        download_root=str(ondl.get("download_root", cfg.download_root)).strip(),
        tools=tools,
        queue=queue,
        preview=preview,
        discord=discord,
    )

    return out, script_dir


def resolve_state_root(cfg: OnDLConfig, *, config_dir: Path) -> Path:
    return _resolve_path(cfg.state_root, base_dir=config_dir) if cfg.state_root else default_state_root()


def resolve_download_root(cfg: OnDLConfig, *, config_dir: Path) -> Path:
    return _resolve_path(cfg.download_root, base_dir=config_dir) if cfg.download_root else default_download_root()
