from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from ..auth import require_auth
from ..flows import list_flows, save_flow, load_flow, delete_flow


router = APIRouter()


@router.get("/flows")
async def api_list_flows(_: bool = Depends(require_auth)):
    return {"items": list_flows()}


@router.get("/flows/{fid}")
async def api_get_flow(fid: str, _: bool = Depends(require_auth)):
    try:
        data = load_flow(fid)
        return data
    except FileNotFoundError:
        return JSONResponse({"error": "not found"}, status_code=404)


@router.post("/flows/{fid}")
async def api_save_flow(fid: str, body: dict, _: bool = Depends(require_auth)):
    save_flow(fid, body)
    return {"ok": True}


@router.delete("/flows/{fid}")
async def api_delete_flow(fid: str, _: bool = Depends(require_auth)):
    delete_flow(fid)
    return {"ok": True}
