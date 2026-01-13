from __future__ import annotations

import os
import shutil
from pathlib import Path


def resolve_tool(exe_name: str, *, env_var: str | None, config_value: str, config_key: str) -> Path:
    """Resolve required tools via: env var -> config path -> PATH."""
    if env_var:
        override = os.environ.get(env_var, "").strip()
        if override:
            p = Path(os.path.expandvars(os.path.expanduser(override)))
            if p.exists():
                return p
            raise RuntimeError(f"Configured tool path for '{exe_name}' not found via {env_var}: {p}")

    if config_value:
        p = Path(os.path.expandvars(os.path.expanduser(config_value)))
        if p.exists():
            return p
        raise RuntimeError(f"Configured tool path for '{exe_name}' not found via [tools].{config_key}: {p}")

    found = shutil.which(exe_name)
    if found:
        return Path(found)

    raise RuntimeError(
        f"Required tool '{exe_name}' not found on PATH. Set {env_var} or [tools].{config_key}."
    )
