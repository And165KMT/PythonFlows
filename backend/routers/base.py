from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, Response, JSONResponse
from pathlib import Path
from ..auth import require_auth
from ..config import kernel_feature_enabled
from ..kernel_runtime import get_kc, start_new_kernel


router = APIRouter()


# Static frontend dir (resolved relative to backend package)
static_dir = Path(__file__).resolve().parents[2] / "frontend"


@router.get("/favicon.ico")
async def favicon():
    ico = static_dir / "favicon.ico"
    svg = static_dir / "favicon.svg"
    if ico.exists():
        return FileResponse(str(ico))
    if svg.exists():
        return FileResponse(str(svg), media_type="image/svg+xml")
    return Response(status_code=204)


@router.get("/health")
async def health():
    kc = get_kc()
    return {"ok": True, "kernel": ("ok" if (kc is not None) else "na")}


@router.get("/")
async def index():
    idx = static_dir / "index.html"
    if idx.exists():
        return FileResponse(str(idx), media_type="text/html")
    return Response(status_code=404)


# --- Fallback minimal APIs to satisfy frontend ---
@router.get("/api/packages")
async def api_packages(_: bool = Depends(require_auth)):
    """Return a static list of built-in packages aligned with routers.packages.

    Keep static UI packages for pandas/sklearn and expose stdlib modules as
    Autogen-only entries (entry == '').
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
    for nm, lab in stdlib_autogen:
        items.append({"name": nm, "label": lab, "entry": ""})
    return items


@router.post("/restart")
async def restart(_: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    try:
        await start_new_kernel()
    except Exception:
        pass
    return {"ok": get_kc() is not None}


@router.post("/bootstrap")
async def bootstrap(_: bool = Depends(require_auth)):
    """Placeholder endpoint for 'Install / Check' button.
    Frontend mostly uses /api/pip/install directly. Here we just reply OK.
    """
    return {"ok": True, "output": "Bootstrap not required. Use Packages to install via PyPI."}
