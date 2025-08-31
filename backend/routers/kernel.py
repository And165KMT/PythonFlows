import asyncio
import json
import time
import uuid
import queue
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse
from ..auth import require_auth, require_ws_auth
from ..config import kernel_feature_enabled, auth_required, exec_timeout_seconds
from ..exec_control import exec_registry, enforce_timeout_and_interrupt
from ..kernel_runtime import get_kc, get_kernel_manager


router = APIRouter()


@router.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    if auth_required():
        ok = await require_ws_auth(ws)
        if not ok:
            await ws.close(code=4401)
            return
    kc = get_kc()
    if not kernel_feature_enabled() or kc is None:
        await ws.close(code=1011)
        return
    try:
        while True:
            try:
                msg = await asyncio.to_thread(kc.get_iopub_msg, 0.2)
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            except Exception:
                await asyncio.sleep(0.05)
                continue
            try:
                await ws.send_text(json.dumps(msg))
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass


@router.post("/run")
async def run_graph(body: dict, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    code = body.get("code")
    if not code:
        return JSONResponse({"error": "no code"}, status_code=400)
    kc = get_kc()
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    msg_id = kc.execute(code)
    if exec_timeout_seconds() > 0:
        try:
            await exec_registry.register(msg_id, time.time())
            asyncio.create_task(enforce_timeout_and_interrupt(get_kernel_manager(), kc, msg_id))
        except Exception:
            pass
    return {"execId": str(uuid.uuid4()), "msgId": msg_id}
