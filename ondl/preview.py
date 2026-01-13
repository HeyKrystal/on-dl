from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

from .util import run


def ffprobe_duration(ffprobe: Path, video_path: Path) -> Optional[float]:
    cmd = [
        str(ffprobe),
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    cp = run(cmd, capture=True, check=False)
    if cp.returncode != 0:
        return None
    try:
        return float(cp.stdout.strip())
    except ValueError:
        return None


def pick_preview_start(duration: Optional[float]) -> float:
    # Simple, stable heuristic: 10% in, clamped 12-30s (matches your current behavior)
    if not duration or duration <= 0:
        return 12.0
    start = duration * 0.10
    return float(min(30.0, max(12.0, start)))


def make_preview_gif(
    ffmpeg: Path,
    video_path: Path,
    out_gif: Path,
    *,
    start_seconds: float,
    seconds: float,
    fps: int,
    width: int,
    max_bytes: int,
) -> Path:
    out_gif.parent.mkdir(parents=True, exist_ok=True)

    # palette path
    palette = out_gif.with_suffix(".palette.png")

    cur_fps = fps
    cur_width = width

    # bound iterations so we never loop forever
    for _ in range(12):
        # 1) palette gen
        run([
            str(ffmpeg),
            "-hide_banner", "-y",
            "-ss", f"{start_seconds:.3f}",
            "-t", f"{seconds:.3f}",
            "-i", str(video_path),
            "-vf", f"fps={cur_fps},scale={cur_width}:-1:flags=lanczos,palettegen",
            str(palette),
        ], capture=True, check=True)

        # 2) gif render
        run([
            str(ffmpeg),
            "-hide_banner", "-y",
            "-ss", f"{start_seconds:.3f}",
            "-t", f"{seconds:.3f}",
            "-i", str(video_path),
            "-i", str(palette),
            #"-lavfi", f"fps={cur_fps},scale={cur_width}:-1:flags=lanczos[x];[x][1:v]paletteuse",
            "-lavfi", f"fps={fps},scale={width}:-1:flags=lanczos [x]; [x][1:v] paletteuse=dither=bayer:bayer_scale=3",
            str(out_gif),
        ], capture=True, check=True)

        size = out_gif.stat().st_size
        if size <= max_bytes:
            try:
                palette.unlink(missing_ok=True)
            except Exception:
                pass
            return out_gif

        # Too big: reduce
        cur_width = max(240, int(cur_width * 0.85))
        cur_fps = max(8, int(math.floor(cur_fps * 0.90)))

    # Give up; return best effort
    try:
        palette.unlink(missing_ok=True)
    except Exception:
        pass
    return out_gif
