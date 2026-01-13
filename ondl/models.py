from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Job:
    url: str
    category: str
    app: str
    raw: dict


@dataclass(frozen=True)
class Meta:
    id: str
    title: str
    uploader: str
    channel: str
    webpage_url: str
    duration: Optional[float]
    thumbnail: Optional[str]
