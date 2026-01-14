from __future__ import annotations

import json
from pathlib import Path

from .models import Meta
from .util import run, sanitize_filename


def get_youtube_meta(ytdlp: Path, url: str) -> Meta:
    cp = run([str(ytdlp), "-J", "--no-playlist", url], capture=True, check=True)
    data = json.loads(cp.stdout)
    return Meta(
        id=str(data.get("id", "")),
        title=str(data.get("title", "")),
        uploader=str(data.get("uploader", "")),
        channel=str(data.get("channel", "")),
        webpage_url=str(data.get("webpage_url", url)),
        duration=float(data["duration"]) if data.get("duration") is not None else None,
        thumbnail=str(data.get("thumbnail", "")) or None,
    )


def download_youtube_to_dir(ytdlp: Path, ffmpeg: Path, url: str, out_dir: Path, archive_path: Path) -> Path:
    """Download to out_dir. Returns the downloaded video path."""
    out_dir.mkdir(parents=True, exist_ok=True)

    output_tmpl = out_dir / "%(title)s.%(ext)s"
    cmd = [
        str(ytdlp),
        "--no-playlist",
        "--ignore-errors",
        "--continue",
        "--write-thumbnail",
        "--convert-thumbnails", "jpg",
        "--download-archive", str(archive_path),
        "--ffmpeg-location", str(ffmpeg),

        # 🔒 Make stdout predictable: only our printed fields
        "-q",
        "--no-warnings",
        "--print", "after_move:filepath",

        "--windows-filenames",
        "-o", str(output_tmpl),
        url,
    ]
    cp = run(cmd, capture=True, check=False)
    if cp.returncode != 0:
        raise RuntimeError(
            f"yt-dlp failed (exit={cp.returncode})\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}"
        )

    # ✅ Primary: yt-dlp told us the final path
    out_lines = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
    if out_lines:
        p = Path(out_lines[-1])

        # Sometimes the printed path can be relative (rare, but cheap to handle)
        if not p.is_absolute():
            p = (out_dir / p).resolve()

        if p.exists():
            tp = p.with_suffix(".jpg")
            if tp.exists():
                poster = tp.with_name(f"{tp.stem}-poster{tp.suffix}")
                tp.rename(poster)
            return p

    # Fallback: scan the directory for the newest non-sidecar file
    vids = sorted(out_dir.glob("*"), key=lambda x: x.stat().st_mtime, reverse=True)
    for f in vids:
        if f.suffix.lower() not in (".jpg", ".json", ".part", ".tmp", ".webp"):
            # try thumbnail rename even in fallback mode
            tp = f.with_suffix(".jpg")
            if tp.exists():
                poster = tp.with_name(f"{tp.stem}-poster{tp.suffix}")
                tp.rename(poster)
            return f

    raise RuntimeError(
        "yt-dlp succeeded but could not locate output file\n"
        f"stdout:\n{cp.stdout}\n"
        f"stderr:\n{cp.stderr}"
    )
