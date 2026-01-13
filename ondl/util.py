from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


def sanitize_filename(s: str) -> str:
    # Conservative: keep alnum, space, dash, underscore; replace others with '_'
    out = []
    for ch in s:
        if ch.isalnum() or ch in (" ", "-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out).strip() or "unknown"


@dataclass(frozen=True)
class CmdResult:
    stdout: str
    stderr: str
    returncode: int


def run(cmd: list[str], *, capture: bool, check: bool) -> CmdResult:
    cp = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        check=False,
    )
    if check and cp.returncode != 0:
        raise subprocess.CalledProcessError(cp.returncode, cmd, output=cp.stdout, stderr=cp.stderr)
    return CmdResult(stdout=cp.stdout or "", stderr=cp.stderr or "", returncode=cp.returncode)
