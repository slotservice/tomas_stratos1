"""
Stratos1 - Build / version info.

Resolves the running build hash (short SHA) so the bot can include it
in the startup notification, audit logs, and any "running build" check.

Resolution order:
1. ``STRATOS1_BUILD`` environment variable (CI/CD / Docker friendly).
2. ``git rev-parse --short HEAD`` in the project directory (works
   when deployed via ``git pull`` like the production server is).
3. Literal string ``"unknown"`` if neither is available.

This is read once at process startup; the value never changes during
a run. Client 2026-05-01 explicit requirement: "Verify that the
running bot is actually the latest commit/build. Startup message
must show the build hash."
"""

from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def get_build_hash() -> str:
    """Return the short build hash for the running bot."""
    env_val = os.environ.get("STRATOS1_BUILD", "").strip()
    if env_val:
        return env_val[:12]

    project_root = Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode == 0:
            sha = result.stdout.strip()
            if sha:
                return sha
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass

    return "unknown"
