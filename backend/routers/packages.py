from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from ..auth import require_auth
from ..config import kernel_feature_enabled
from ..kernel_runtime import get_kernel_manager, get_kc, start_new_kernel


router = APIRouter()


@router.get("/api/packages")
async def api_packages(_: bool = Depends(require_auth)):
    """Return the list of logical packages that the frontend can toggle.

    The frontend treats packages in two ways:
    - Static UI packages with JS under frontend/packages/<name>/index.js
    - Autogen-only logical packages where we do not ship JS; toggling triggers
      /api/introspect_module on the Python module to generate nodes.

    Here we expose common stdlib modules as Autogen targets so users can import
    and use basics (json, os, re, etc.) from the right pane. We also keep
    pandas/sklearn as static UI packages.
    """
    stdlib_autogen = [
        ("json", "JSON"),
        ("os", "OS"),
        ("io", "IO"),
        ("re", "Regex"),
        ("math", "Math"),
        ("statistics", "Statistics"),
        ("random", "Random"),
        ("itertools", "Itertools"),
        ("functools", "Functools"),
        ("datetime", "Datetime"),
        ("pathlib", "Pathlib"),
        ("csv", "CSV"),
        ("glob", "Glob"),
        ("shutil", "Shutil"),
        ("subprocess", "Subprocess"),
        ("tarfile", "Tarfile"),
        ("zipfile", "Zipfile"),
        ("pickle", "Pickle"),
        ("textwrap", "Textwrap"),
        ("urllib.request", "Urllib"),
    ]
    items = [
        {"name": "pandas", "label": "Pandas", "entry": "index.js"},
        {"name": "sklearn", "label": "Sklearn", "entry": "index.js"},
    ]
    # Autogen-only entries do not have JS entry files
    for nm, lab in stdlib_autogen:
        items.append({"name": nm, "label": lab, "entry": ""})
    return items


@router.post("/restart")
async def restart_kernel(_: bool = Depends(require_auth)):
    """Restart the Jupyter kernel if kernel feature is enabled."""
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    # If there's an active kernel client/manager, start_new_kernel() will
    # terminate and recreate it.
    try:
        await start_new_kernel()
    except Exception:
        # As a fallback, try to at least signal current kernel state in response
        pass
    ok = get_kc() is not None
    return {"ok": ok}
