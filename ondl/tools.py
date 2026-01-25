from __future__ import annotations

import os
import shutil
from pathlib import Path


def resolve_tool(exe_name: str, *, env_var: str | None, config_value: str | None, config_key: str) -> Path:
    """Resolve required tools via: env var -> config path -> PATH."""

    # Environment variable (if provided and non-empty)
    if env_var:
        override = os.environ.get(env_var)
        if override:
            override = override.strip()
            if override:
                p = Path(os.path.expandvars(os.path.expanduser(override)))
                if p.exists():
                    return p
                raise RuntimeError(
                    f"Configured tool path for '{exe_name}' not found via {env_var}: {p}"
                )

    # Config value (ONLY if non-empty)
    if config_value:
        config_value = config_value.strip()
        if config_value:
            p = Path(os.path.expandvars(os.path.expanduser(config_value)))
            if p.exists():
                return p
            raise RuntimeError(
                f"Configured tool path for '{exe_name}' not found via [tools].{config_key}: {p}"
            )

    # PATH lookup
    found = shutil.which(exe_name)
    if found:
        return Path(found)

    # Hard failure
    raise RuntimeError(
        f"Required tool '{exe_name}' not found on PATH. "
        f"Set {env_var} or [tools].{config_key}."
    )
