from __future__ import annotations

import errno
import os
import shutil
import uuid
from pathlib import Path


def _safe_move_file(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Try atomic replace (works when src and dest are on same filesystem)
    try:
        os.replace(src, dest)
        return
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise

    # Cross-device move: copy to temp file in destination dir, then atomic replace
    tmp = dest.with_name(dest.name + f".tmp-{uuid.uuid4().hex}")
    shutil.copy2(src, tmp)
    os.replace(tmp, dest)
    src.unlink(missing_ok=True)


def move_merge(src: Path, dest: Path) -> None:
    """Move file/dir into dest, merging directories recursively.

    File overwrites are done safely (atomic replace when possible).
    """
    if src.is_dir():
        dest.mkdir(parents=True, exist_ok=True)
        for child in src.iterdir():
            move_merge(child, dest / child.name)
        try:
            src.rmdir()
        except OSError:
            pass
        return

    _safe_move_file(src, dest)
