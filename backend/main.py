from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import asyncio
import json
import uuid
import nest_asyncio
from jupyter_client.manager import KernelManager
from pathlib import Path
import queue
import subprocess
import sys
from typing import Optional

nest_asyncio.apply()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

kernel_manager: Optional[KernelManager] = None
kc = None  # type: ignore

# Static frontend dir
static_dir = Path(__file__).resolve().parent.parent / "frontend"

@app.get("/")
async def index():
    return FileResponse(static_dir / "index.html")

app.mount("/static", StaticFiles(directory=str(static_dir), html=False), name="static")
app.mount("/pkg", StaticFiles(directory=str(static_dir / "packages"), html=False), name="packages")

@app.get("/api/packages")
async def list_packages():
    pkgs = []
    pkg_dir = static_dir / "packages"
    if pkg_dir.exists():
        for child in pkg_dir.iterdir():
            if child.is_dir() and (child / "index.js").exists():
                pkgs.append({"name": child.name, "label": child.name.capitalize(), "entry": "index.js"})
    return pkgs

@app.on_event("startup")
async def startup():
    await _start_new_kernel()

@app.on_event("shutdown")
async def shutdown():
    global kernel_manager, kc
    if kc:
        kc.stop_channels()
    if kernel_manager:
        kernel_manager.shutdown_kernel(now=True)

@app.post("/run")
async def run_graph(body: dict):
    """Run a tiny fixed Pandas flow on the kernel and return an execution id.
    For MVP: CSV -> Select -> GroupBy -> Plot (saved to a PNG file path and return path)
    """
    exec_id = str(uuid.uuid4())
    code = body.get("code")
    if not code:
        return JSONResponse({"error": "no code"}, status_code=400)
    # send code for execution
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    msg_id = kc.execute(code)
    return {"execId": exec_id, "msgId": msg_id}

@app.get("/health")
async def health():
    ok = bool(kernel_manager)
    return {"kernel": "ok" if ok else "down"}

@app.post("/restart")
async def restart_kernel():
    """Restart the single Jupyter kernel."""
    global kernel_manager, kc
    try:
        if kc:
            kc.stop_channels()
    except Exception:
        pass
    try:
        if kernel_manager:
            kernel_manager.shutdown_kernel(now=True)
    except Exception:
        pass
    # start a fresh kernel
    await _start_new_kernel()
    return {"ok": True}

async def _start_new_kernel():
    global kernel_manager, kc
    kernel_manager = KernelManager()
    assert kernel_manager is not None
    kernel_manager.start_kernel()
    kc = kernel_manager.client()
    kc.start_channels()

@app.post("/bootstrap")
async def bootstrap():
    """Attempt to install backend requirements into the current venv.
    This is best-effort for local convenience.
    """
    req = Path(__file__).resolve().parent / "requirements.txt"
    cmd = [sys.executable, "-m", "pip", "install", "-r", str(req)]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return {"ok": True, "output": out[-2000:]}
    except subprocess.CalledProcessError as e:
        return JSONResponse({"ok": False, "output": (e.output or "")[-2000:]}, status_code=500)

@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            # Read iopub messages and forward minimal fields
            try:
                if kc is None:
                    await asyncio.sleep(0.1)
                    continue
                msg = kc.get_iopub_msg(timeout=0.1)
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            mtype = msg.get("header", {}).get("msg_type")
            content = msg.get("content", {})
            data = {"type": mtype, "content": content}
            await ws.send_text(json.dumps(data, default=str))
    except WebSocketDisconnect:
        pass
    except Exception:
        await ws.close()
