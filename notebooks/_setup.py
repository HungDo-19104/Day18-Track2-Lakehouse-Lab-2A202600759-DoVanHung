"""Path bootstrap for the lightweight notebooks.

Resolves `scripts/lakehouse.py` from the repo root regardless of where
Jupyter / Python was launched from. Used by all NB*/lite notebooks:

    import _setup  # noqa: F401  -- adds scripts/ to sys.path
    from lakehouse import path, reset

Why: the prior pattern `sys.path.insert(0, "../scripts")` is *cwd-relative*
and silently breaks if the notebook is run from the repo root or a CI
runner. `__file__` is stable; cwd is not.
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DOCKER = Path("/workspace/scripts")
_LOCAL = _HERE.parent / "scripts"

_TARGET = _DOCKER if _DOCKER.exists() else _LOCAL
sys.path.insert(0, str(_TARGET))
