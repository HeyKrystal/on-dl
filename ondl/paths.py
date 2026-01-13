from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from .config import default_download_root


YTDLP_ARCHIVE_FILENAME = "ytdlp-archive.txt"


@dataclass(frozen=True)
class Paths:
    state_root: Path
    download_root: Path
    staging_root: Path
    fallback_download_root: Path
    incoming: Path
    processing: Path
    done: Path
    error: Path
    logs: Path
    previews: Path
    archive: Path


def build_paths(state_root: Path, download_root: Path) -> Paths:
    staging_root = state_root / "staging-downloads"
    fallback = default_download_root()

    return Paths(
        state_root=state_root,
        download_root=download_root,
        staging_root=staging_root,
        fallback_download_root=fallback,
        incoming=state_root / "incoming",
        processing=state_root / "processing",
        done=state_root / "done",
        error=state_root / "error",
        logs=state_root / "logs",
        previews=state_root / "previews",
        archive=state_root / YTDLP_ARCHIVE_FILENAME,
    )
