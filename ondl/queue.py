from __future__ import annotations

import json
import time
from pathlib import Path

from .models import Job


def ensure_dirs(paths) -> None:
    for p in (paths.incoming, paths.processing, paths.done, paths.error, paths.logs, paths.previews, paths.staging_root):
        p.mkdir(parents=True, exist_ok=True)


def parse_job_file(job_path: Path) -> Job:
    raw = json.loads(job_path.read_text(encoding="utf-8"))
    return Job(
        url=str(raw.get("url", "")).strip(),
        category=str(raw.get("category", "")).strip(),
        app=str(raw.get("app", "")).strip(),
        raw=raw,
    )


def claim_job(paths, job_path: Path) -> Path:
    """Atomically move a job from incoming to processing."""
    dest = paths.processing / job_path.name
    job_path.replace(dest)
    return dest


def finish_job(paths, job_path: Path, ok: bool) -> Path:
    dest_dir = paths.done if ok else paths.error
    dest = dest_dir / job_path.name
    job_path.replace(dest)
    return dest


def reap_stale_processing_jobs(paths, *, job_glob: str, stale_minutes: int, action: str) -> int:
    if stale_minutes <= 0:
        return 0
    action = action.strip().lower()
    if action not in ("requeue", "error"):
        raise ValueError("queue.stale_processing_action must be 'requeue' or 'error'")

    cutoff = time.time() - (stale_minutes * 60)
    moved = 0
    for job in paths.processing.glob(job_glob):
        try:
            mtime = job.stat().st_mtime
        except FileNotFoundError:
            continue
        if mtime >= cutoff:
            continue

        target_dir = paths.incoming if action == "requeue" else paths.error
        target = target_dir / job.name
        try:
            job.replace(target)
            moved += 1
        except FileNotFoundError:
            pass
    return moved
