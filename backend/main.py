from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
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
import os
import time
from .config import kernel_feature_enabled, auth_required, get_api_token, exec_timeout_seconds, export_max_rows_default
from .auth import require_auth, require_ws_auth
from .flows import list_flows, save_flow, load_flow, delete_flow
from .flow_models import FlowModel
try:
    from pydantic import ValidationError  # type: ignore
except Exception:  # fallback if pydantic v1 already available without import path
    ValidationError = Exception  # type: ignore
from .exec_control import exec_registry, enforce_timeout_and_interrupt
import re

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
# Gate to coordinate exclusive iopub reads between websocket stream and API calls
iopub_gate = asyncio.Lock()

# Static frontend dir
static_dir = Path(__file__).resolve().parent.parent / "frontend"
root_dir = Path(__file__).resolve().parent.parent

def _uploads_dir() -> Path:
    d = root_dir / "data" / "uploads"
    d.mkdir(parents=True, exist_ok=True)
    return d

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
def _safe_filename(name: str) -> str:
    name = Path(name).name  # strip path
    if not _SAFE_NAME.match(name):
        stem = re.sub(r"[^A-Za-z0-9_.-]", "_", name)[:120] or "file"
        ext = ''.join(Path(name).suffixes)
        return (stem + ext)[:128]
    return name

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
    if kernel_feature_enabled():
        await _start_new_kernel()
    else:
        print("[Kernel] Feature disabled; running without Jupyter kernel")

@app.on_event("shutdown")
async def shutdown():
    global kernel_manager, kc
    if kc:
        kc.stop_channels()
    if kernel_manager:
        kernel_manager.shutdown_kernel(now=True)

@app.post("/run")
async def run_graph(body: dict, _: bool = Depends(require_auth)):
    """Run a tiny fixed Pandas flow on the kernel and return an execution id.
    For MVP: CSV -> Select -> GroupBy -> Plot (saved to a PNG file path and return path)
    """
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    # optional auth
    # use dependency manually to avoid changing handler signature
    try:
        await asyncio.to_thread(lambda: None)
    except Exception:
        pass
    exec_id = str(uuid.uuid4())
    code = body.get("code")
    if not code:
        return JSONResponse({"error": "no code"}, status_code=400)
    # send code for execution
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    msg_id = kc.execute(code)
    # Register timeout watcher if configured
    if exec_timeout_seconds() > 0:
        try:
            await exec_registry.register(msg_id, time.time())
            asyncio.create_task(enforce_timeout_and_interrupt(kernel_manager, kc, msg_id))
        except Exception:
            pass
    return {"execId": exec_id, "msgId": msg_id}

@app.get("/api/variables")
async def list_variables(_: bool = Depends(require_auth)):
    """Execute a short snippet on the kernel to list global variables and return as JSON."""
    if not kernel_feature_enabled():
        return {"variables": []}
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json, types\n"
        "def __fp_list_vars():\n"
        "    out=[]\n"
        "    for k,v in list(globals().items()):\n"
        "        if str(k).startswith('_'):\n"
        "            continue\n"
        "        # Skip modules and callables early to avoid heavy repr()\n"
        "        try:\n"
        "            if isinstance(v, types.ModuleType) or callable(v):\n"
        "                continue\n"
        "        except Exception:\n"
        "            pass\n"
        "        try:\n"
        "            t = type(v).__name__\n"
        "        except Exception:\n"
        "            t = 'unknown'\n"
        "        try:\n"
        "            r = repr(v)[:200]\n"
        "        except Exception:\n"
        "            r = '<unrepr>'\n"
    "        html = None\n"
    "        rows = None\n"
    "        cols = None\n"
    "        shape = None\n"
        "        try:\n"
        "            if t == 'DataFrame':\n"
    "                _df = v.head(5)\n"
    "                try:\n"
    "                    rows, cols = v.shape\n"
    "                except Exception:\n"
    "                    rows, cols = None, None\n"
    "                html = _df.to_html(index=False, border=0)\n"
    "            elif t == 'ndarray':\n"
    "                try:\n"
    "                    shape = tuple(v.shape)\n"
    "                except Exception:\n"
    "                    shape = None\n"
        "        except Exception:\n"
    "            html = None\n"
    "        out.append({'name': str(k), 'type': t, 'repr': r, 'html': html, 'rows': rows, 'cols': cols, 'shape': shape})\n"
        "    print('[[VARS]]'+json.dumps(out))\n"
        "__fp_list_vars()\n"
    )
    vars_json = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        # Wait for output related to this execution
        deadline = time.time() + 3.0
        try:
            while time.time() < deadline:
                try:
                    msg = kc.get_iopub_msg(timeout=0.2)
                except queue.Empty:
                    continue
                # Only consider messages for our execution
                if msg.get("parent_header", {}).get("msg_id") != msg_id:
                    continue
                mtype = msg.get("header", {}).get("msg_type")
                content = msg.get("content", {})
                if mtype == "stream" and content.get("text", "").startswith("[[VARS]]"):
                    payload = content.get("text", "")[8:]
                    try:
                        vars_json = json.loads(payload)
                    except Exception:
                        vars_json = []
                    # We have the payload; return early to reduce latency
                    break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return {"variables": vars_json or []}

@app.get("/health")
async def health():
    if not kernel_feature_enabled():
        return {"kernel": "disabled", "auth": "required" if auth_required() else "optional"}
    ok = bool(kernel_manager)
    return {"kernel": "ok" if ok else "down", "auth": "required" if auth_required() else "optional"}

@app.post("/restart")
async def restart_kernel(_: bool = Depends(require_auth)):
    """Restart the single Jupyter kernel."""
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
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
    # If a Jupyter Enterprise Gateway URL is provided, route kernel operations through it.
    # This enables using a remote kernel hosted in Azure (e.g., AKS) transparently.
    gw_url = os.environ.get("JUPYTER_GATEWAY_URL")
    if gw_url:
        try:
            import importlib
            mod = importlib.import_module('jupyter_client.gateway')
            GatewayClient = getattr(mod, 'GatewayClient', None)
            if GatewayClient is not None:
                gc = GatewayClient.instance()
                gc.url = gw_url
                auth = os.environ.get("JUPYTER_GATEWAY_AUTH_TOKEN")
                if auth:
                    gc.auth_token = auth
                print(f"[Kernel] Using Jupyter Gateway: {gw_url}")
            else:
                print("[Kernel] jupyter_client.gateway not available; falling back to local kernel")
        except Exception as e:
            print(f"[Kernel] Failed to configure Jupyter Gateway ({e}); falling back to local kernel")
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

# ------------------- Uploads API -------------------

@app.get("/api/uploads")
async def api_list_uploads(_: bool = Depends(require_auth)):
    d = _uploads_dir()
    items = []
    for p in d.iterdir():
        if p.is_file():
            try:
                st = p.stat()
                items.append({
                    "name": p.name,
                    "size": int(st.st_size),
                    "mtime": int(st.st_mtime),
                })
            except Exception:
                pass
    items.sort(key=lambda x: x["name"].lower())
    return {"items": items}

@app.post("/api/uploads")
async def api_upload_file(file: UploadFile = File(...), _: bool = Depends(require_auth)):
    try:
        name = _safe_filename(file.filename or "file")
        dest = _uploads_dir() / name
        # stream write
        with dest.open("wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
        abs_path = str(dest.resolve())
        return {"ok": True, "name": name, "path": abs_path}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/uploads/{name}")
async def api_get_upload(name: str, _: bool = Depends(require_auth)):
    safe = _safe_filename(name)
    p = _uploads_dir() 
    fp = (p / safe)
    if not fp.exists() or not fp.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(str(fp))

@app.delete("/api/uploads/{name}")
async def api_delete_upload(name: str, _: bool = Depends(require_auth)):
    safe = _safe_filename(name)
    fp = _uploads_dir() / safe
    if fp.exists():
        try:
            fp.unlink()
            return {"ok": True}
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"error": "not found"}, status_code=404)

@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    if not kernel_feature_enabled():
        await ws.accept()
        await ws.send_text(json.dumps({"type": "error", "content": {"message": "kernel feature disabled"}}))
        await ws.close()
        return
    # auth if required
    if not await require_ws_auth(ws):
        return
    await ws.accept()
    try:
        while True:
            # Read iopub messages and forward minimal fields
            try:
                if kc is None:
                    await asyncio.sleep(0.1)
                    continue
                # If another endpoint is consuming iopub exclusively, wait
                if iopub_gate.locked():
                    await asyncio.sleep(0.05)
                    continue
                msg = kc.get_iopub_msg(timeout=0.1)
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            mtype = msg.get("header", {}).get("msg_type")
            content = msg.get("content", {})
            # Resolve timeout tracking when we observe idle for a parent msg
            try:
                if mtype == "status" and content.get("execution_state") == "idle":
                    mid = msg.get("parent_header", {}).get("msg_id")
                    if mid:
                        await exec_registry.resolve(mid)
            except Exception:
                pass
            data = {"type": mtype, "content": content}
            await ws.send_text(json.dumps(data, default=str))
    except WebSocketDisconnect:
        pass
    except Exception:
        await ws.close()


# ------------------- Flows API -------------------

@app.get("/api/flows")
async def api_list_flows(_: bool = Depends(require_auth)):
    return {"items": list_flows()}


@app.get("/api/flows/{name}.json")
async def api_get_flow(name: str, _: bool = Depends(require_auth)):
    try:
        data = load_flow(name)
        # validate and possibly coerce
        model = FlowModel.parse_obj(data)
        return model.dict(by_alias=True)
    except FileNotFoundError:
        return JSONResponse({"error": "not found"}, status_code=404)


@app.post("/api/flows/{name}.json")
async def api_save_flow(name: str, body: dict, _: bool = Depends(require_auth)):
    try:
        # validate before saving
        FlowModel.parse_obj(body)
        p = save_flow(name, body)
        return {"ok": True, "path": str(p)}
    except ValidationError as e:  # type: ignore
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/flows/{name}.json")
async def api_delete_flow(name: str, _: bool = Depends(require_auth)):
    try:
        ok = delete_flow(name)
        if ok:
            return {"ok": True}
        return JSONResponse({"error": "not found"}, status_code=404)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


# ------------------- Variable export -------------------

@app.get("/api/variables/{name}/export")
async def export_variable(name: str, format: str = "csv", rows: Optional[int] = None, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    fmt = (format or "csv").lower()
    max_rows = export_max_rows_default()
    nrows = max(1, min(max_rows, int(rows or max_rows)))
    # Build code to serialize the variable
    code = (
        "import json, io\n"
        "import types\n"
        "def __fp_export(name, fmt, nrows):\n"
        "    try:\n"
        "        import pandas as pd\n"
        "    except Exception:\n"
        "        pd = None\n"
        "    try:\n"
        "        import numpy as np\n"
        "    except Exception:\n"
        "        np = None\n"
        "    v = globals().get(name, None)\n"
        "    if fmt=='csv':\n"
        "        try:\n"
        "            if pd is not None and getattr(v, '__class__', None) is not None and v.__class__.__name__=='DataFrame':\n"
        "                buf = io.StringIO()\n"
        "                v.head(int(nrows)).to_csv(buf, index=False)\n"
        "                print('[[EXPORT:CSV]]'+buf.getvalue())\n"
        "                return\n"
        "        except Exception:\n"
        "            pass\n"
        "        try:\n"
        "            if np is not None and hasattr(v, 'shape'):\n"
        "                arr = v\n"
        "                try:\n"
        "                    import numpy as _np\n"
        "                    arr = _np.array(v)\n"
        "                except Exception:\n"
        "                    pass\n"
        "                s = arr.shape if hasattr(arr, 'shape') else None\n"
        "                if s is None:\n"
        "                    print('[[EXPORT:TEXT]]'+str(v))\n"
        "                    return\n"
        "                # handle 1D/2D; higher dims -> reshape to 2D\n"
        "                try:\n"
        "                    if len(s)==1:\n"
        "                        arr2 = arr.reshape(-1,1)\n"
        "                    elif len(s)>=2:\n"
        "                        arr2 = arr.reshape(s[0], -1)\n"
        "                    else:\n"
        "                        arr2 = arr\n"
        "                    arr2 = arr2[:int(nrows)]\n"
        "                    buf = io.StringIO()\n"
        "                    for r in arr2:\n"
        "                        try:\n"
        "                            it = list(r)\n"
        "                        except Exception:\n"
        "                            it = [r]\n"
        "                        buf.write(','.join(str(x) for x in it))\n"
        "                        buf.write('\n')\n"
        "                    print('[[EXPORT:CSV]]'+buf.getvalue())\n"
        "                    return\n"
        "                except Exception:\n"
        "                    pass\n"
        "        except Exception:\n"
        "            pass\n"
        "    # fallback\n"
        "    try:\n"
        "        print('[[EXPORT:TEXT]]'+repr(v))\n"
        "    except Exception:\n"
        "        print('[[EXPORT:TEXT]]<unrepr>')\n"
        f"__fp_export('{name}', '{fmt}', {nrows})\n"
    )
    payload = []
    kind = None  # 'CSV' or 'TEXT'
    deadline = time.time() + 5.0
    async with iopub_gate:
        msg_id = kc.execute(code)
        if exec_timeout_seconds() > 0:
            try:
                await exec_registry.register(msg_id, time.time())
                asyncio.create_task(enforce_timeout_and_interrupt(kernel_manager, kc, msg_id))
            except Exception:
                pass
        try:
            while time.time() < deadline:
                try:
                    msg = kc.get_iopub_msg(timeout=0.2)
                except queue.Empty:
                    continue
                if msg.get("parent_header", {}).get("msg_id") != msg_id:
                    continue
                mtype = msg.get("header", {}).get("msg_type")
                content = msg.get("content", {})
                if mtype == "stream":
                    text = content.get("text", "")
                    if text.startswith("[[EXPORT:CSV]]"):
                        payload.append(text[len("[[EXPORT:CSV]]"):])
                        kind = "CSV"
                    elif text.startswith("[[EXPORT:TEXT]]"):
                        payload.append(text[len("[[EXPORT:TEXT]]"):])
                        kind = "TEXT"
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    data = "".join(payload)
    if not data:
        return JSONResponse({"error": "no data"}, status_code=500)
    filename = f"{name}.{ 'csv' if kind=='CSV' else 'txt'}"
    media = "text/csv" if kind == "CSV" else "text/plain"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(iter([data]), media_type=media, headers=headers)
