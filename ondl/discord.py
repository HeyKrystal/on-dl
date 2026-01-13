from __future__ import annotations

import json
import mimetypes
import sys
import uuid
import urllib.request
from pathlib import Path
from typing import Optional
from ondl.config import DiscordConfig # Can I do ondl.config instead?



def discord_post(cfg: DiscordConfig, *, content: str, embed: dict, gif_path: Optional[Path]) -> None:
    if not cfg.webhook_url:
        return

    boundary = "----ondl-" + uuid.uuid4().hex
    parts: list[bytes] = []

    payload = {"content": content, "embeds": [embed]}
    if cfg.username:
        payload["username"] = cfg.username
    if cfg.avatar_url:
        payload["avatar_url"] = cfg.avatar_url

    def add_field(name: str, value: bytes, content_type: str | None = None, filename: str | None = None) -> None:
        header = f"--{boundary}\r\n"
        header += f'Content-Disposition: form-data; name="{name}"'
        if filename:
            header += f'; filename="{filename}"'
        header += "\r\n"
        if content_type:
            header += f"Content-Type: {content_type}\r\n"
        header += "\r\n"
        parts.append(header.encode("utf-8") + value + b"\r\n")

    add_field("payload_json", json.dumps(payload).encode("utf-8"), "application/json")

    if gif_path and gif_path.exists():
        mt = mimetypes.guess_type(gif_path.name)[0] or "application/octet-stream"
        add_field("files[0]", gif_path.read_bytes(), mt, gif_path.name)

    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)

    req = urllib.request.Request(
        cfg.webhook_url,
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": "OnDL (github.com/HeyKrystal/on-dl)",
        }, 
        method="POST",
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        # read response body to see Discord’s JSON error message
        msg = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        print(f"[discord] HTTPError {e.code}: {msg}", file=sys.stderr)
        return

