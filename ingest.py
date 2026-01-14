#!/usr/bin/env python3
"""OnDL ingest entrypoint.

Intended to be called via iOS Shortcut -> SSH:

    echo "$B64PAYLOAD" | /path/to/ingest.py

STDIN: base64-encoded JSON payload (single line) OR raw JSON.
Output (stdout): one line status for Shortcuts:
    - ALREADY_DOWNLOADED
    - ALREADY_QUEUED
    - QUEUED <filename>
    - ERROR <message>

Exit codes:
    0 = success (queued or already)
    2 = invalid payload / cannot parse
    3 = internal error
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

from ondl.config import load_config, resolve_download_root, resolve_state_root
from ondl.paths import build_paths
from ondl.tools import resolve_tool


def _read_stdin_all() -> str:
    data = sys.stdin.read()
    return data.strip()


def _maybe_b64_decode(s: str) -> str:
    """If s looks like base64, decode it; otherwise return as-is."""
    if s.lstrip().startswith("{"):
        return s
    try:
        padded = s + ("=" * ((4 - (len(s) % 4)) % 4))
        raw = base64.b64decode(padded.encode("utf-8"), validate=True)
        return raw.decode("utf-8")
    except Exception:
        return s


def _parse_payload(text: str) -> dict[str, Any]:
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("payload must be a JSON object")
    return obj


def _normalize_app(app: str) -> str:
    return (app or "").strip()


def _extract_url(payload: dict[str, Any]) -> str:
    url = str(payload.get("url", "")).strip()
    if not url:
        raise ValueError("payload missing 'url'")
    return url


def _archive_keyline(ytdlp: Path, url: str) -> Optional[str]:
    """Return '<extractor_key> <id>' for yt-dlp archive matching, or None."""
    import subprocess

    cmd = [str(ytdlp), "-s", "--print", "%(extractor_key)s %(id)s", url]
    cp = subprocess.run(cmd, capture_output=True, text=True)
    if cp.returncode != 0:
        return None
    
    lines = (cp.stdout or "").splitlines()
    if not lines:
        return None
    
    # Normalize ONLY the extractor key (case-insensitive), keep id case-sensitive
    parts = lines[0].strip().split(maxsplit=1)
    if len(parts) != 2:
        return None

    extractor, video_id = parts
    return f"{extractor.lower()} {video_id}"


def _archive_contains(archive_path: Path, keyline: str) -> bool:

    print(f"Test: {keyline}")
    try:
        with archive_path.open("r", encoding="utf-8", errors="replace") as f:
            for ln in f:
                if ln.rstrip("\n") == keyline:
                    return True
    except FileNotFoundError:
        return False
    return False


def _already_queued(paths, job_glob: str, url: str) -> bool:
    """Scan incoming + processing for a job with same url."""
    for d in (paths.incoming, paths.processing):
        for p in d.glob(job_glob):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if str(data.get("url", "")).strip() == url:
                    return True
            except Exception:
                continue
    return False


def _enqueue_atomic(incoming_dir: Path, *, filename: str, payload_text: str) -> Path:
    incoming_dir.mkdir(parents=True, exist_ok=True)
    final = incoming_dir / filename
    tmp = incoming_dir / (".tmp-" + filename + f"-{os.getpid()}")
    tmp.write_text(payload_text + "\n", encoding="utf-8")
    tmp.replace(final)
    return final


def main() -> int:
    try:
        raw_in = _read_stdin_all()
        if not raw_in:
            print("ERROR empty stdin")
            return 2

        payload_text = _maybe_b64_decode(raw_in)
        payload = _parse_payload(payload_text)

        url = _extract_url(payload)
        app = _normalize_app(str(payload.get("app", "")).strip())
        if not app:
            app = "YouTube"
            payload["app"] = app

        cfg, cfg_dir = load_config(Path(__file__))
        state_root = resolve_state_root(cfg, config_dir=cfg_dir)
        download_root = resolve_download_root(cfg, config_dir=cfg_dir)
        paths = build_paths(state_root, download_root)

        ytdlp = resolve_tool("yt-dlp", env_var="ONDL_YTDLP", config_value=cfg.tools.ytdlp, config_key="ytdlp")

        if _already_queued(paths, cfg.queue.job_glob, url):
            print("ALREADY_QUEUED")
            return 0

        keyline = _archive_keyline(ytdlp, url)
        if keyline and _archive_contains(paths.archive, keyline):
            print("ALREADY_DOWNLOADED")
            return 0

        ts = time.strftime("%Y%m%d-%H%M%S")
        rand = os.getpid() ^ int(time.time() * 1000)
        filename = f"{ts}-{rand}.dljob"

        payload["url"] = url
        payload["app"] = app

        final = _enqueue_atomic(paths.incoming, filename=filename, payload_text=json.dumps(payload, ensure_ascii=False))
        print(f"QUEUED {final.name}")
        return 0

    except (ValueError, json.JSONDecodeError) as e:
        print(f"ERROR {e}")
        return 2
    except Exception as e:
        print(f"ERROR internal: {e}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
