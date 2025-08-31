from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from ..auth import require_auth
from ..config import kernel_feature_enabled
from ..kernel_runtime import get_kernel_manager, get_kc, start_new_kernel


router = APIRouter()


@router.get("/api/packages")
async def api_packages(_: bool = Depends(require_auth)):
    """Return the list of frontend JS packages to load.

    Shape expected by frontend:
      [ { name: string, label: string, entry: string } ]

    We ship built-in packages in frontend/packages/* with an index.js entry.
    """
    # Built-in packages present in the repo
    items = [
        {"name": "python", "label": "Python", "entry": "index.js"},
        {"name": "pandas", "label": "Pandas", "entry": "index.js"},
        {"name": "sklearn", "label": "Sklearn", "entry": "index.js"},
    ]
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
