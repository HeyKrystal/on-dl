#!/usr/bin/env python3
from __future__ import annotations

import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

from ondl.config import load_config, resolve_download_root, resolve_state_root
from ondl.paths import build_paths
from ondl.tools import resolve_tool
from ondl.queue import ensure_dirs, reap_stale_processing_jobs, parse_job_file, claim_job, finish_job
from ondl.media import get_youtube_meta, download_youtube_to_dir
from ondl.preview import ffprobe_duration, pick_preview_start, make_preview_gif
from ondl.discord import discord_post
from ondl.fs import move_merge

def main() -> int:
    cfg, cfg_dir = load_config(Path(__file__))
    state_root = resolve_state_root(cfg, config_dir=cfg_dir)
    download_root = resolve_download_root(cfg, config_dir=cfg_dir)
    paths = build_paths(state_root, download_root)

    # Tools
    ytdlp = resolve_tool("yt-dlp", env_var="ONDL_YTDLP", config_value=cfg.tools.ytdlp, config_key="ytdlp")
    ffmpeg = resolve_tool("ffmpeg", env_var="ONDL_FFMPEG", config_value=cfg.tools.ffmpeg, config_key="ffmpeg")
    ffprobe = resolve_tool("ffprobe", env_var="ONDL_FFPROBE", config_value=cfg.tools.ffprobe, config_key="ffprobe")

    ensure_dirs(paths)

    # Reaper
    reaped = reap_stale_processing_jobs(
        paths,
        job_glob=cfg.queue.job_glob,
        stale_minutes=cfg.queue.stale_processing_minutes,
        action=cfg.queue.stale_processing_action,
    )
    if reaped:
        print(f"[reaper] moved {reaped} stale job(s) out of processing")

    processed = 0
    print(f"== consume start {datetime.now().isoformat()} ==")

    for job_file in sorted(paths.incoming.glob(cfg.queue.job_glob)):
        if processed >= cfg.queue.max_per_run:
            break

        def fmt_dur(seconds: Optional[float]) -> str:
            if not seconds:
                return "unknown"
            s = int(seconds)
            h, s = divmod(s, 3600)
            m, s = divmod(s, 60)
            return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"

        claimed = None
        ok = False
        try:
            claimed = claim_job(paths, job_file)
            job = parse_job_file(claimed)

            if job.app.lower() != "youtube":
                raise RuntimeError(f"Unsupported app right now: {job.app}")

            meta = get_youtube_meta(ytdlp, job.url)

            # staging layout: <category>/<uploader>/
            staging_dir = paths.staging_root / (meta.uploader or "unknown")
            video_path = download_youtube_to_dir(ytdlp, ffmpeg, job.url, staging_dir, paths.archive)
            size_mb = video_path.stat().st_size / (1024 * 1024)

            duration = ffprobe_duration(ffprobe, video_path)
            start = pick_preview_start(duration)
            gif_path = paths.previews / f"{video_path.stem}.gif"
            gif_path = make_preview_gif(
                ffmpeg,
                video_path,
                gif_path,
                start_seconds=start,
                seconds=cfg.preview.gif_seconds,
                fps=cfg.preview.gif_fps,
                width=cfg.preview.gif_width,
                max_bytes=cfg.preview.gif_max_bytes,
            )

            # Move bundle (directory) into final destination
            dest_dir = paths.download_root / job.category / (meta.uploader or "unknown")
            try:
                move_merge(staging_dir, dest_dir)
                used_fallback = False
            except Exception:
                # Fallback to local Downloads/OnDL
                fallback_dir = paths.fallback_download_root / job.category / (meta.uploader or "unknown")
                move_merge(staging_dir, fallback_dir)
                used_fallback = True
                dest_dir = fallback_dir

            # Discord
            success_green = 0x2ECC71
            warning_yellow = 0xF1C40F
            embed = {
                "title": meta.title,
                "url": meta.webpage_url,
                "description": f"Downloaded and rendered preview (starts at ~{start:.2f}s).",
                "color": success_green,
                "author": {
                    "name": "📥 DOWNLOAD FINISHED",
                    "icon_url": cfg.discord.author_icon_url,
                },
                "footer": {
                    "text": "FrostedStoat • OnDL"
                    #"icon_url": "https://i.imgur.com/AfFp7pu.png"
                },
                "fields": [
                    {"name": "Channel", "value": meta.channel or meta.uploader or "unknown", "inline": True},
                    {"name": "Duration", "value": fmt_dur(meta.duration), "inline": True},
                    {"name": "File Size", "value": f"{size_mb:.2f} MB", "inline": True},
                    {"name": "Saved To", "value": str(dest_dir), "inline": False},
                ],
            }

            if used_fallback:
                embed["fields"].append({
                    "name": "⚠️ Storage Fallback Used",
                    "value": "Download root unavailable; file saved locally. Check logs for details.",
                    "inline": False,
                })
                embed["color"] = warning_yellow

            if meta.thumbnail:
                embed["thumbnail"] = {"url": meta.thumbnail}
                
            if gif_path is not None:
                embed["image"] = {"url": f"attachment://{gif_path.name}"}

            try:
                discord_post(cfg.discord, content="", embed=embed, gif_path=gif_path)
            except Exception as e:
                # Discord notifications must never fail the job.
                # Log and continue.
                err_detail = f"{type(e).__name__}: {e}"

                # If it's a urllib HTTP error, try to show status + response body
                try:
                    import urllib.error
                    if isinstance(e, urllib.error.HTTPError):
                        body = ""
                        try:
                            body = e.read().decode("utf-8", errors="replace")
                        except Exception:
                            body = ""
                        err_detail = f"HTTP {e.code} {e.reason}; body={body[:500]!r}"
                except Exception:
                    pass

                print(f"[warn] Discord webhook failed: {err_detail}", file=sys.stderr)


            ok = True
        except Exception:
            traceback.print_exc(file=sys.stderr)
            ok = False
        finally:
            if claimed:
                finish_job(paths, claimed, ok)
            processed += 1

    print(f"== consume end {datetime.now().isoformat()} processed={processed} ==")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
