from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import asyncio
import json
import uuid
from jupyter_client.manager import KernelManager
from pathlib import Path
import queue
import subprocess
import sys
from typing import Optional
import os
import time
from .config import (
    kernel_feature_enabled,
    auth_required,
    get_api_token,
    exec_timeout_seconds,
    export_max_rows_default,
    variables_list_timeout_seconds,
    export_timeout_seconds,
)
from .auth import require_auth, require_ws_auth
from .flows import list_flows, save_flow, load_flow, delete_flow
from .flow_models import FlowModel
# Prefer Pydantic v1-compatible ValidationError if available (avoids v2 warnings)
try:
    from pydantic.v1 import ValidationError  # type: ignore
except Exception:
    try:
        from pydantic import ValidationError  # type: ignore
    except Exception:  # last-resort fallback
        ValidationError = Exception  # type: ignore
from .exec_control import exec_registry, enforce_timeout_and_interrupt
import re


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

# Optional auto-generated node specs endpoint.
# Frontend tries to fetch this at startup; return an empty list when not configured
# to avoid noisy 404s in the browser console.
@app.get("/api/autogen")
async def api_autogen():
    return {"nodes": []}

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
async def list_variables(pattern: Optional[str] = None, include_private: bool = False, _: bool = Depends(require_auth)):
    """Execute a short snippet on the kernel to list global variables and return as JSON."""
    if not kernel_feature_enabled():
        return {"variables": []}
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    # Build python that runs in the kernel to safely summarize variables.
    pattern_js = pattern or ""
    include_private_flag = "True" if include_private else "False"
    code = (
        "import json, types, re, sys\n"
        f"__pat = r'''{pattern_js}'''\n"
        f"__incl_priv = {include_private_flag}\n"
        "def __fp_list_vars():\n"
        "    out=[]\n"
        "    rgx = re.compile(__pat) if __pat else None\n"
        "    for k,v in list(globals().items()):\n"
        "        if not __incl_priv and str(k).startswith('_'):\n"
        "            continue\n"
        "        if rgx and not rgx.search(str(k)):\n"
        "            continue\n"
        "        # Skip modules and callables early to avoid heavy repr()\n"
        "        try:\n"
        "            import types as _types\n"
        "            if isinstance(v, _types.ModuleType) or callable(v):\n"
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
        "        size_bytes = None\n"
        "        dtype = None\n"
        "        dtypes = None\n"
        "        columns = None\n"
        "        length = None\n"
        "        try:\n"
        "            if hasattr(v, '__len__'):\n"
        "                try: length = int(len(v))\n"
        "                except Exception: length = None\n"
        "        except Exception: pass\n"
        "        try:\n"
        "            if t == 'DataFrame':\n"
        "                _df = v.head(5)\n"
        "                try:\n"
        "                    rows, cols = v.shape\n"
        "                except Exception:\n"
        "                    rows, cols = None, None\n"
        "                try:\n"
        "                    columns = [str(c) for c in list(v.columns[:50])]\n"
        "                    dtypes = {str(c): str(v[c].dtype) for c in v.columns[:50]}\n"
        "                except Exception:\n"
        "                    columns, dtypes = None, None\n"
        "                try:\n"
        "                    size_bytes = int(getattr(v.memory_usage(deep=True), 'sum', lambda: 0)())\n"
        "                except Exception:\n"
        "                    size_bytes = None\n"
        "                html = _df.to_html(index=False, border=0)\n"
        "            elif t == 'Series':\n"
        "                try: rows = int(v.shape[0])\n"
        "                except Exception: rows = length\n"
        "                try: dtype = str(v.dtype)\n"
        "                except Exception: pass\n"
        "                try: size_bytes = int(v.memory_usage(deep=True))\n"
        "                except Exception: pass\n"
        "                try: html = v.head(5).to_frame().to_html(index=False, border=0)\n"
        "                except Exception: html = None\n"
        "            elif t == 'ndarray':\n"
        "                try: shape = tuple(v.shape)\n"
        "                except Exception: shape = None\n"
        "                try: dtype = str(v.dtype)\n"
        "                except Exception: pass\n"
        "                try: size_bytes = int(getattr(v, 'nbytes', 0))\n"
        "                except Exception: pass\n"
        "            else:\n"
        "                try: size_bytes = int(sys.getsizeof(v))\n"
        "                except Exception: size_bytes = None\n"
        "        except Exception:\n"
        "            pass\n"
        "        out.append({'name': str(k), 'type': t, 'repr': r, 'html': html, 'rows': rows, 'cols': cols, 'shape': shape, 'size_bytes': size_bytes, 'dtype': dtype, 'dtypes': dtypes, 'columns': columns, 'length': length})\n"
        "    print('[[VARS]]'+json.dumps(out))\n"
        "__fp_list_vars()\n"
    )
    vars_json = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        # Wait for output related to this execution
        deadline = time.time() + float(variables_list_timeout_seconds())
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

# ------------------- Variable detail/preview management -------------------

@app.get("/api/variables/{name}")
async def variable_detail(name: str, stats: bool = False, limit_cols: int = 50, limit_rows: int = 20, _: bool = Depends(require_auth)):
    """Return a structured summary for a single variable without changing the UI.
    For DataFrame: columns, dtypes (truncated), optional numeric describe when stats=true.
    Response shape: { name, type, rows, cols, shape, columns, dtypes, sample: {columns, data} }
    """
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json, math\n"
        f"__name = r'''{name}'''\n"
        f"__limit_cols = int({max(1, int(limit_cols or 50))})\n"
        f"__limit_rows = int({max(1, int(limit_rows or 20))})\n"
        f"__want_stats = {bool(stats)}\n"
        "def _emit(obj):\n"
        "    print('[[VAR:DETAIL]]'+json.dumps(obj))\n"
        "v = globals().get(__name, None)\n"
        "if v is None:\n"
        "    _emit({'error':'not found'})\n"
        "else:\n"
        "    out={'name': __name, 'type': type(v).__name__}\n"
        "    try:\n"
        "        import pandas as pd\n"
        "        import numpy as np\n"
        "    except Exception:\n"
        "        pd = None; np = None\n"
        "    try:\n"
        "        if pd is not None and isinstance(v, pd.DataFrame):\n"
        "            try: out['rows'], out['cols'] = int(v.shape[0]), int(v.shape[1])\n"
        "            except Exception: pass\n"
        "            try:\n"
        "                cols = [str(c) for c in list(v.columns[:__limit_cols])]\n"
        "                dtypes = {str(c): str(v[c].dtype) for c in v.columns[:__limit_cols]}\n"
        "                out.update({'columns': cols, 'dtypes': dtypes})\n"
        "            except Exception: pass\n"
        "            try:\n"
        "                _samp = v.head(__limit_rows)\n"
        "                out['sample'] = {'columns':[str(c) for c in _samp.columns], 'data': [[None if (isinstance(x,float) and (math.isnan(x) or math.isinf(x))) else x for x in row] for row in _samp.to_numpy().tolist()]}\n"
        "            except Exception: pass\n"
        "            if __want_stats:\n"
        "                try:\n"
        "                    desc = v.describe(include='all').to_dict()\n"
        "                    out['stats'] = desc\n"
        "                except Exception: pass\n"
        "        elif pd is not None and isinstance(v, pd.Series):\n"
        "            try: out['rows'] = int(v.shape[0])\n"
        "            except Exception: pass\n"
        "            try: out['dtype'] = str(v.dtype)\n"
        "            except Exception: pass\n"
        "            try:\n"
        "                _samp = v.head(__limit_rows)\n"
        "                out['sample'] = {'columns':['value'], 'data': [[x] for x in _samp.tolist()]}\n"
        "            except Exception: pass\n"
        "        elif np is not None and hasattr(v, 'shape') and hasattr(v, 'dtype'):\n"
        "            try: out['shape'] = tuple(int(x) for x in v.shape)\n"
        "            except Exception: pass\n"
        "            try: out['dtype'] = str(v.dtype)\n"
        "            except Exception: pass\n"
        "            try:\n"
        "                arr=v\n"
        "                if len(getattr(arr,'shape',[]))==1:\n"
        "                    arr2 = arr.reshape(-1,1)\n"
        "                else:\n"
        "                    s = arr.shape; arr2 = arr.reshape(s[0], -1)\n"
        "                arr2 = arr2[:__limit_rows]\n"
        "                out['sample'] = {'columns':[f'c{i}' for i in range(arr2.shape[1])], 'data': arr2.tolist()}\n"
        "            except Exception: pass\n"
        "    except Exception: pass\n"
        "    _emit(out)\n"
    )
    data = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + float(variables_list_timeout_seconds())
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
                    if text.startswith("[[VAR:DETAIL]]"):
                        payload = text[len("[[VAR:DETAIL]]"):]
                        try:
                            data = json.loads(payload)
                        except Exception:
                            data = None
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}

@app.get("/api/variables/{name}/head")
async def variable_head(name: str, rows: int = 20, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        f"__name = r'''{name}'''\n"
        f"__rows = int({max(1, int(rows or 20))})\n"
        "def _emit(obj): print('[[VAR:HEAD]]'+json.dumps(obj))\n"
        "v = globals().get(__name, None)\n"
        "if v is None: _emit({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v, pd.DataFrame):\n"
        "      df=v.head(__rows)\n"
        "      _emit({'columns':[str(c) for c in df.columns], 'data': df.to_numpy().tolist()})\n"
        "    elif isinstance(v, pd.Series):\n"
        "      s=v.head(__rows)\n"
        "      _emit({'columns':['value'], 'data': [[x] for x in s.tolist()]})\n"
        "    else:\n"
        "      raise Exception('not df/series')\n"
        "  except Exception:\n"
        "    try:\n"
        "      import numpy as np\n"
        "      if hasattr(v,'shape'):\n"
        "        arr=np.array(v)\n"
        "        arr2=arr.reshape(arr.shape[0], -1) if arr.ndim>1 else arr.reshape(-1,1)\n"
        "        arr2=arr2[:__rows]\n"
        "        _emit({'columns':[f'c{i}' for i in range(arr2.shape[1])], 'data': arr2.tolist()})\n"
        "      else:\n"
        "        _emit({'columns':['repr'], 'data': [[repr(v)]]})\n"
        "    except Exception:\n"
        "      _emit({'columns':['repr'], 'data': [[repr(v)]]})\n"
    )
    data = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + float(variables_list_timeout_seconds())
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
                    if text.startswith("[[VAR:HEAD]]"):
                        payload = text[len("[[VAR:HEAD]]"):]
                        try:
                            data = json.loads(payload)
                        except Exception:
                            data = None
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}

@app.get("/api/variables/{name}/sample")
async def variable_sample(name: str, rows: int = 50, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        f"__name = r'''{name}'''\n"
        f"__rows = int({max(1, int(rows or 50))})\n"
        "def _emit(obj): print('[[VAR:SAMPLE]]'+json.dumps(obj))\n"
        "v = globals().get(__name, None)\n"
        "if v is None: _emit({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v, pd.DataFrame):\n"
        "      df=v.sample(min(__rows, len(v))) if len(v)>__rows else v.copy()\n"
        "      _emit({'columns':[str(c) for c in df.columns], 'data': df.to_numpy().tolist()})\n"
        "    elif isinstance(v, pd.Series):\n"
        "      s=v.sample(min(__rows, len(v))) if len(v)>__rows else v.copy()\n"
        "      _emit({'columns':['value'], 'data': [[x] for x in s.tolist()]})\n"
        "    else:\n"
        "      raise Exception('not df/series')\n"
        "  except Exception:\n"
        "    _emit({'error':'unsupported'})\n"
    )
    data = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + float(variables_list_timeout_seconds())
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
                    if text.startswith("[[VAR:SAMPLE]]"):
                        payload = text[len("[[VAR:SAMPLE]]"):]
                        try:
                            data = json.loads(payload)
                        except Exception:
                            data = None
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}

@app.delete("/api/variables/{name}")
async def variable_delete(name: str, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        f"__n = r'''{name}'''\n"
        "if __n in globals():\n"
        "    globals().pop(__n, None); print('[[VAR:DEL]]ok')\n"
        "else:\n"
        "    print('[[VAR:DEL]]missing')\n"
    )
    result = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + 3.0
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
                    if text.startswith("[[VAR:DEL]]"):
                        result = text.split("]]")[-1]
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    if result == "ok":
        return {"ok": True}
    elif result == "missing":
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse({"error": "timeout"}, status_code=504)

@app.post("/api/variables/{name}/rename")
async def variable_rename(name: str, to: Optional[str] = Form(None), body: Optional[dict] = None, _: bool = Depends(require_auth)):
    if body and not to:
        to = body.get("to")  # type: ignore
    if not to:
        return JSONResponse({"error": "missing 'to'"}, status_code=400)
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        f"__src = r'''{name}'''\n"
        f"__dst = r'''{to}'''\n"
        "if __src in globals():\n"
        "    try:\n"
        "        globals()[__dst] = globals().pop(__src)\n"
        "        print('[[VAR:REN]]ok')\n"
        "    except Exception as e:\n"
        "        print('[[VAR:REN]]err:'+str(e))\n"
        "else:\n"
        "    print('[[VAR:REN]]missing')\n"
    )
    res = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + 4.0
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
                    if text.startswith("[[VAR:REN]]"):
                        res = text[len("[[VAR:REN]]"):]
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    if res == "ok":
        return {"ok": True}
    if res == "missing":
        return JSONResponse({"error": "not found"}, status_code=404)
    if res and res.startswith("err:"):
        return JSONResponse({"error": res[4:]}, status_code=500)
    return JSONResponse({"error": "timeout"}, status_code=504)

# ------------------- Module versions -------------------

@app.get("/api/modules/versions")
async def api_module_versions(names: str = "", _: bool = Depends(require_auth)):
    """Return versions for comma-separated module names by importing them in the kernel.
    Response: { items: [{ name, version }] }
    """
    if not kernel_feature_enabled():
        return {"items": []}
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    raw = str(names or "").strip()
    mods = [s.strip() for s in raw.split(',') if s.strip()]
    if not mods:
        return {"items": []}
    # Compose kernel code
    esc = lambda s: s.replace("'", "\\'")
    list_repr = ",".join([f"'{esc(m)}'" for m in mods])
    code = (
        "import json, importlib\n"
        f"__mods = [{list_repr}]\n"
        "__out = []\n"
        "for __m in __mods:\n"
        "    try:\n"
        "        _mod = importlib.import_module(__m)\n"
        "        _ver = getattr(_mod, '__version__', None)\n"
        "        __out.append({'name': __m, 'version': str(_ver) if _ver is not None else None})\n"
        "    except Exception:\n"
        "        __out.append({'name': __m, 'version': None})\n"
        "print('[[VERSIONS]]' + json.dumps(__out))\n"
    )
    items = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + 4.0
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
                    if text.startswith("[[VERSIONS]]"):
                        payload = text[len("[[VERSIONS]]"):]
                        try:
                            items = json.loads(payload)
                        except Exception:
                            items = []
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return {"items": items or []}

# ------------------- Imports (in-kernel registry) -------------------

@app.get("/api/imports")
async def api_list_imports(_: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"items": []}
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        "try:\n"
        "    d = globals().get('__pf_imports', {})\n"
        "    print('[[IMPORTS]]'+json.dumps(d))\n"
        "except Exception:\n"
        "    print('[[IMPORTS]]{}')\n"
    )
    items = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + 3.0
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
                    if text.startswith("[[IMPORTS]]"):
                        payload = text[len("[[IMPORTS]]"):]
                        try:
                            obj = json.loads(payload)
                            # normalize to array of {name, alias, version}
                            if isinstance(obj, dict):
                                items = [{"name": k, "alias": (v or {}).get("alias"), "version": (v or {}).get("version")} for k, v in obj.items()]
                            else:
                                items = []
                        except Exception:
                            items = []
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return {"items": items or []}

# ------------------- Introspection APIs -------------------

def _introspect_module_code(mod: str, include: str, exclude: str, limit: int) -> str:
    # Python snippet executed in kernel to inspect module and emit NodeSpec list
    return (
        "import json, importlib, inspect, re\n"
        f"_modname = r'''{mod}'''\n"
        f"_inc = re.compile(r'''{include or '.*'}''')\n"
        f"_exc = re.compile(r'''{exclude or '^_'}''')\n"
        f"_limit = int({max(1, int(limit or 50))})\n"
        "_out = []\n"
        "def _param_specs(obj):\n"
        "  try:\n"
        "    sig = inspect.signature(obj)\n"
        "  except Exception:\n"
        "    return []\n"
        "  ps = []\n"
        "  for n, p in sig.parameters.items():\n"
        "    if n in ('self','cls'): continue\n"
        "    if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD): continue\n"
        "    d = None\n"
        "    if p.default is not inspect._empty:\n"
        "      try:\n"
        "        d = p.default if isinstance(p.default,(str,int,float,bool)) else str(p.default)\n"
        "      except Exception:\n"
        "        d = str(p.default)\n"
        "    ps.append({'name': n, 'default': d, 'ui': 'string'})\n"
        "  return ps\n"
        "try:\n"
        "  M = importlib.import_module(_modname)\n"
        "  items = []\n"
        "  _root = _modname.split('.')[0] if _modname else 'autogen'\n"
        "  _cat = (_modname.split('.')[-1] if '.' in _modname else _root).capitalize()\n"
        "  for name, obj in inspect.getmembers(M):\n"
        "    if not _inc.search(name): continue\n"
        "    if _exc.search(name): continue\n"
        "    try:\n"
    "      if inspect.isfunction(obj):\n"
    "        params = _param_specs(obj)\n"
    "        _m = getattr(obj, '__module__', _modname) or _modname\n"
    "        _root2 = (_m.split('.')[0] if _m else _root)\n"
    "        _cat2 = (_m.split('.')[-1] if _m else _cat).capitalize()\n"
        "        _out.append({\n"
        "          'id': f'autogen.{_modname}.{name}',\n"
        "          'title': name,\n"
    "          'category': _cat2,\n"
        "          'inputType': 'Any',\n"
        "          'outputType': 'Any',\n"
        "          'params': params,\n"
    "          'pkg': _root2,\n"
        "          'call': { 'target': f'{_modname}.{name}', 'kind': 'function', 'receiver': None, 'dfParam': None, 'returnsSelf': False }\n"
        "        })\n"
        "      elif inspect.isclass(obj):\n"
    "        params = _param_specs(obj)\n"
    "        _m = getattr(obj, '__module__', _modname) or _modname\n"
    "        _root2 = (_m.split('.')[0] if _m else _root)\n"
    "        _cat2 = (_m.split('.')[-1] if _m else _cat).capitalize()\n"
        "        # constructor\n"
        "        _out.append({\n"
        "          'id': f'autogen.{_modname}.{name}',\n"
        "          'title': name,\n"
    "          'category': 'Estimator' if hasattr(obj, 'fit') else _cat2,\n"
        "          'inputType': 'Any',\n"
        "          'outputType': 'Estimator' if hasattr(obj, 'fit') else 'Any',\n"
        "          'params': params,\n"
    "          'pkg': _root2,\n"
        "          'call': { 'target': f'{_modname}.{name}', 'kind': 'constructor', 'receiver': None, 'dfParam': None, 'returnsSelf': False }\n"
        "        })\n"
        "        # fit/predict shortcuts if available\n"
        "        if hasattr(obj, 'fit'):\n"
        "          _out.append({\n"
        "            'id': f'autogen.{_modname}.{name}.fit',\n"
        "            'title': f'{name}.fit',\n"
        "            'category': 'Estimator',\n"
        "            'inputType': 'Any',\n"
        "            'outputType': 'Estimator',\n"
    "            'params': _param_specs(getattr(obj,'fit', None)) if hasattr(obj,'fit') else [],\n"
    "            'pkg': _root2,\n"
        "            'call': { 'target': f'{_modname}.{name}.fit', 'kind': 'method', 'receiver': 'estimator', 'dfParam': 'X', 'returnsSelf': True }\n"
        "          })\n"
        "        if hasattr(obj, 'predict'):\n"
        "          _out.append({\n"
        "            'id': f'autogen.{_modname}.{name}.predict',\n"
        "            'title': f'{name}.predict',\n"
        "            'category': 'Estimator',\n"
        "            'inputType': 'Any',\n"
        "            'outputType': 'Any',\n"
    "            'params': _param_specs(getattr(obj,'predict', None)) if hasattr(obj,'predict') else [],\n"
    "            'pkg': _root2,\n"
        "            'call': { 'target': f'{_modname}.{name}.predict', 'kind': 'method', 'receiver': 'estimator', 'dfParam': 'X', 'returnsSelf': False }\n"
        "          })\n"
        "    except Exception:\n"
        "      pass\n"
        "  _out = _out[:_limit]\n"
        "  print('[[INTROSPECT]]' + json.dumps({'nodes': _out}))\n"
        "except Exception as e:\n"
        "  print('[[INTROSPECT]]' + json.dumps({'nodes': []}))\n"
    )


@app.get("/api/introspect_module")
async def api_introspect_module(module: str, include: Optional[str] = None, exclude: Optional[str] = r"^_", limit: int = 50, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"nodes": []}
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = _introspect_module_code(module, include or "", exclude or r"^_", limit or 50)
    data = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + 8.0
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
                    if text.startswith("[[INTROSPECT]]"):
                        payload = text[len("[[INTROSPECT]]"):]
                        try:
                            data = json.loads(payload)
                        except Exception:
                            data = None
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    if isinstance(data, dict) and isinstance(data.get("nodes"), list):
        return data
    return {"nodes": []}


@app.get("/api/introspect")
async def api_introspect_target(target: str, _: bool = Depends(require_auth)):
    """Introspect a fully-qualified name. If it is a module, delegate to module introspection.
    If it is a class, returns constructor + fit/predict method specs when present. If function, returns single node.
    """
    if not kernel_feature_enabled():
        return {"nodes": []}
    # build small code to detect and produce a minimal node list
    code = (
        "import json, importlib, inspect\n"
        f"_tgt = r'''{target}'''\n"
        "def _emit(nodes):\n"
        "  print('[[INTROSPECT]]' + json.dumps({'nodes': nodes}))\n"
        "def _param_specs(obj):\n"
        "  try:\n"
        "    sig = inspect.signature(obj)\n"
        "  except Exception:\n"
        "    return []\n"
        "  ps = []\n"
        "  for n, p in sig.parameters.items():\n"
        "    if n in ('self','cls'): continue\n"
        "    if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD): continue\n"
        "    d = None\n"
        "    if p.default is not inspect._empty:\n"
        "      try:\n"
        "        d = p.default if isinstance(p.default,(str,int,float,bool)) else str(p.default)\n"
        "      except Exception:\n"
        "        d = str(p.default)\n"
        "    ps.append({'name': n, 'default': d, 'ui': 'string'})\n"
        "  return ps\n"
        "try:\n"
        "  parts = _tgt.split('.')\n"
        "  root = parts[0]; modpath = '.'.join(parts[:-1])\n"
        "  M = importlib.import_module(modpath) if modpath else importlib.import_module(root)\n"
        "  name = parts[-1] if parts else ''\n"
        "  obj = getattr(M, name) if name else M\n"
        "  out = []\n"
        "  if inspect.ismodule(obj):\n"
        "    out = []\n"
        "  elif inspect.isclass(obj):\n"
        "    cname = obj.__name__\n"
        "    _cat = (modpath.split('.')[-1] if modpath else root).capitalize()\n"
        "    out.append({'id': f'autogen.{modpath}.{cname}', 'title': cname, 'category': 'Estimator' if hasattr(obj,'fit') else _cat, 'inputType':'Any', 'outputType': 'Estimator' if hasattr(obj,'fit') else 'Any', 'params': _param_specs(obj), 'pkg': root, 'call': {'target': f'{modpath}.{cname}', 'kind':'constructor', 'receiver': None, 'dfParam': None, 'returnsSelf': False}})\n"
        "    if hasattr(obj, 'fit'): out.append({'id': f'autogen.{modpath}.{cname}.fit', 'title': f'{cname}.fit', 'category': 'Estimator', 'inputType':'Any', 'outputType':'Estimator', 'params': _param_specs(getattr(obj,'fit', None)), 'pkg': root, 'call': {'target': f'{modpath}.{cname}.fit', 'kind':'method', 'receiver':'estimator', 'dfParam':'X', 'returnsSelf': True}})\n"
        "    if hasattr(obj, 'predict'): out.append({'id': f'autogen.{modpath}.{cname}.predict', 'title': f'{cname}.predict', 'category': 'Estimator', 'inputType':'Any', 'outputType':'Any', 'params': _param_specs(getattr(obj,'predict', None)), 'pkg': root, 'call': {'target': f'{modpath}.{cname}.predict', 'kind':'method', 'receiver':'estimator', 'dfParam':'X', 'returnsSelf': False}})\n"
        "  elif inspect.isfunction(obj):\n"
        "    fname = obj.__name__\n"
        "    _cat = (modpath.split('.')[-1] if modpath else root).capitalize()\n"
        "    out.append({'id': f'autogen.{modpath}.{fname}', 'title': fname, 'category': _cat, 'inputType':'Any', 'outputType':'Any', 'params': _param_specs(obj), 'pkg': root, 'call': {'target': _tgt, 'kind':'function', 'receiver': None, 'dfParam': None, 'returnsSelf': False}})\n"
        "  _emit(out)\n"
        "except Exception:\n"
        "  _emit([])\n"
    )
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    data = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + 8.0
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
                    if text.startswith("[[INTROSPECT]]"):
                        payload = text[len("[[INTROSPECT]]"):]
                        try:
                            data = json.loads(payload)
                        except Exception:
                            data = None
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    if isinstance(data, dict) and isinstance(data.get("nodes"), list):
        return data
    return {"nodes": []}

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
        "import json, io, base64\n"
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
        "    if fmt in ('csv','json','jsonl'):\n"
        "        try:\n"
        "            if pd is not None and getattr(v, '__class__', None) is not None and v.__class__.__name__=='DataFrame':\n"
        "                if fmt=='csv':\n"
        "                    buf = io.StringIO()\n"
        "                    v.head(int(nrows)).to_csv(buf, index=False)\n"
        "                    print('[[EXPORT:CSV]]'+buf.getvalue()); return\n"
        "                elif fmt=='json':\n"
        "                    js = v.head(int(nrows)).to_json(orient='records')\n"
        "                    print('[[EXPORT:JSON]]'+js); return\n"
        "                elif fmt=='jsonl':\n"
        "                    js = v.head(int(nrows)).to_json(orient='records', lines=True)\n"
        "                    print('[[EXPORT:JSONL]]'+js); return\n"
        "        except Exception: pass\n"
        "        try:\n"
        "            if np is not None and hasattr(v, 'shape'):\n"
        "                arr = v\n"
        "                try:\n"
        "                    import numpy as _np\n"
        "                    arr = _np.array(v)\n"
        "                except Exception: pass\n"
        "                s = arr.shape if hasattr(arr, 'shape') else None\n"
        "                if s is None:\n"
        "                    print('[[EXPORT:TEXT]]'+str(v)); return\n"
        "                try:\n"
        "                    if len(s)==1: arr2 = arr.reshape(-1,1)\n"
        "                    elif len(s)>=2: arr2 = arr.reshape(s[0], -1)\n"
        "                    else: arr2 = arr\n"
        "                    arr2 = arr2[:int(nrows)]\n"
        "                    if fmt=='csv':\n"
        "                        buf = io.StringIO()\n"
        "                        for r in arr2:\n"
        "                            try: it = list(r)\n"
        "                            except Exception: it = [r]\n"
        "                            buf.write(','.join(str(x) for x in it)); buf.write('\n')\n"
        "                        print('[[EXPORT:CSV]]'+buf.getvalue()); return\n"
        "                    else:\n"
        "                        # json/jsonl\n"
        "                        import json as _json\n"
        "                        rows = arr2.tolist()\n"
        "                        if fmt=='jsonl':\n"
        "                            s = '\n'.join(_json.dumps(r) for r in rows)\n"
        "                            print('[[EXPORT:JSONL]]'+s); return\n"
        "                        s = _json.dumps(rows)\n"
        "                        print('[[EXPORT:JSON]]'+s); return\n"
        "                except Exception: pass\n"
        "        except Exception: pass\n"
        "    if fmt in ('parquet','pickle','pkl','npy'):\n"
        "        try:\n"
        "            if fmt in ('parquet','pickle','pkl') and pd is not None and getattr(v,'__class__',None) is not None and v.__class__.__name__=='DataFrame':\n"
        "                buf = io.BytesIO()\n"
        "                if fmt=='parquet':\n"
        "                    try:\n"
        "                        v.head(int(nrows)).to_parquet(buf, index=False)\n"
        "                        print('[[EXPORT:B64:parquet]]'+base64.b64encode(buf.getvalue()).decode('ascii')); return\n"
        "                    except Exception as e: pass\n"
        "                else:\n"
        "                    v.head(int(nrows)).to_pickle(buf)\n"
        "                    print('[[EXPORT:B64:pkl]]'+base64.b64encode(buf.getvalue()).decode('ascii')); return\n"
        "            if fmt=='npy' and np is not None and hasattr(v,'shape'):\n"
        "                import numpy as _np\n"
        "                arr=_np.array(v)\n"
        "                if arr.ndim>1: arr = arr[:int(nrows)]\n"
        "                buf = io.BytesIO(); _np.save(buf, arr)\n"
        "                print('[[EXPORT:B64:npy]]'+base64.b64encode(buf.getvalue()).decode('ascii')); return\n"
        "        except Exception: pass\n"
        "    # fallback\n"
        "    try: print('[[EXPORT:TEXT]]'+repr(v))\n"
        "    except Exception: print('[[EXPORT:TEXT]]<unrepr>')\n"
        f"__fp_export('{name}', '{fmt}', {nrows})\n"
    )
    payload = []
    kind = None  # 'CSV' | 'TEXT' | 'JSON' | 'JSONL' | 'B64:<ext>'
    deadline = time.time() + float(export_timeout_seconds())
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
                    elif text.startswith("[[EXPORT:JSONL]]"):
                        payload.append(text[len("[[EXPORT:JSONL]]"):])
                        kind = "JSONL"
                    elif text.startswith("[[EXPORT:JSON]]"):
                        payload.append(text[len("[[EXPORT:JSON]]"):])
                        kind = "JSON"
                    elif text.startswith("[[EXPORT:B64:"):
                        # format [[EXPORT:B64:ext]]<base64>
                        try:
                            tag, b64 = text.split("]]", 1)
                            ext = tag.split(":")[-1]
                            payload.append(json.dumps({"ext": ext, "b64": b64}))
                            kind = f"B64:{ext}"
                        except Exception:
                            pass
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
    # Handle binary (base64) case
    if kind and kind.startswith("B64:"):
        try:
            obj = json.loads(data)
            ext = obj.get("ext", "bin")
            raw = obj.get("b64", "")
            import base64 as _b64
            bin_data = _b64.b64decode(raw)
            media = "application/octet-stream"
            headers = {"Content-Disposition": f"attachment; filename={name}.{ext}"}
            return StreamingResponse(iter([bin_data]), media_type=media, headers=headers)
        except Exception:
            return JSONResponse({"error": "decode failed"}, status_code=500)
    # Text/CSV/JSON/JSONL
    if kind == "CSV":
        filename = f"{name}.csv"; media = "text/csv"
    elif kind == "JSON":
        filename = f"{name}.json"; media = "application/json"
    elif kind == "JSONL":
        filename = f"{name}.jsonl"; media = "application/x-ndjson"
    else:
        filename = f"{name}.txt"; media = "text/plain"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(iter([data]), media_type=media, headers=headers)

# ------------------- Deep-dive DataFrame utilities -------------------

def _deadline() -> float:
    try:
        return time.time() + float(variables_list_timeout_seconds())
    except Exception:
        return time.time() + 5.0


def _await_json(tag: str, msg_id):
    data = None
    deadline = _deadline()
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
                if text.startswith(tag):
                    payload = text[len(tag):]
                    try:
                        data = json.loads(payload)
                    except Exception:
                        data = None
                    break
            elif mtype == "status" and content.get("execution_state") == "idle":
                break
    except Exception:
        pass
    return data


@app.get("/api/variables/{name}/tail")
async def variable_tail(name: str, rows: int = 20, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        f"__name=r'''{name}'''\n"
        f"__rows=int({max(1, int(rows or 20))})\n"
        "def _e(o): print('[[VAR:TAIL]]'+json.dumps(o))\n"
        "v=globals().get(__name,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v, pd.DataFrame): df=v.tail(__rows); _e({'columns':[str(c) for c in df.columns],'data':df.to_numpy().tolist()})\n"
        "    elif isinstance(v, pd.Series): s=v.tail(__rows); _e({'columns':['value'],'data':[[x] for x in s.tolist()]})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:TAIL]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}

# ------------------- JSON/XML response analysis -------------------

@app.get("/api/variables/{name}/detect_format")
async def variable_detect_format(name: str, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json, xml.etree.ElementTree as ET\n"
        f"__n=r'''{name}'''\n"
        "def _e(o): print('[[VAR:DETECT]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  t=type(v).__name__\n"
        "  try:\n"
        "    if isinstance(v,(dict,list)): _e({'format':'json','rootType': t}); raise SystemExit\n"
        "  except Exception: pass\n"
        "  try:\n"
        "    s = v.decode('utf-8','ignore') if isinstance(v,(bytes,bytearray)) else str(v)\n"
        "    try:\n"
        "      obj=json.loads(s)\n"
        "      _e({'format':'json','rootType': type(obj).__name__}); raise SystemExit\n"
        "    except Exception: pass\n"
        "    try:\n"
        "      ET.fromstring(s)\n"
        "      _e({'format':'xml','rootType':'xml'})\n"
        "    except Exception:\n"
        "      _e({'format':'text','rootType': t})\n"
        "  except Exception:\n"
        "    _e({'format':'unknown','rootType': t})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:DETECT]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/json/preview")
async def variable_json_preview(name: str, path: Optional[str] = None, limit: int = 50, flatten: bool = True, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    p = path or ""
    code = (
        "import json\n"
        f"__n=r'''{name}'''\n"
        f"__path=r'''{p}'''\n"
        f"__lim=int({max(1,int(limit or 50))})\n"
        f"__flatten={bool(flatten)}\n"
        "def _e(o): print('[[VAR:JPREV]]'+json.dumps(o))\n"
        "def _parse_json(v):\n"
        "  try:\n"
        "    if isinstance(v,(dict,list)): return v\n"
        "    s=v.decode('utf-8','ignore') if isinstance(v,(bytes,bytearray)) else str(v)\n"
        "    return json.loads(s)\n"
        "  except Exception: return None\n"
        "def _nav(obj, path):\n"
        "  if not path: return obj\n"
        "  cur=obj\n"
        "  for tok in path.split('.'):\n"
        "    if tok=='': continue\n"
        "    if '[' in tok and tok.endswith(']'):\n"
        "      key, idx = tok.split('[',1)[0], tok.split('[',1)[1][:-1]\n"
        "      if key: cur = (cur.get(key) if isinstance(cur, dict) else None)\n"
        "      try: idx=int(idx)\n"
        "      except Exception: return None\n"
        "      try: cur = cur[idx]\n"
        "      except Exception: return None\n"
        "    else:\n"
        "      cur = cur.get(tok) if isinstance(cur, dict) else None\n"
        "    if cur is None: return None\n"
        "  return cur\n"
        "v=globals().get(__n,None)\n"
        "obj=_parse_json(v)\n"
        "if obj is None: _e({'error':'not json'})\n"
        "else:\n"
        "  tgt=_nav(obj,__path)\n"
        "  if tgt is None: _e({'error':'path not found'})\n"
        "  elif isinstance(tgt,list):\n"
        "    rows=tgt[:__lim]\n"
        "    if __flatten and all(isinstance(r,dict) for r in rows):\n"
        "      cols=set()\n"
        "      for r in rows: cols.update(r.keys())\n"
        "      cols=list(cols)[:100]\n"
        "      data=[[r.get(c) if not isinstance(r.get(c),(dict,list)) else json.dumps(r.get(c)) for c in cols] for r in rows]\n"
        "      _e({'kind':'list<object>','columns':[str(c) for c in cols],'data':data,'total':len(tgt)})\n"
        "    else:\n"
        "      _e({'kind':'list','sample': rows, 'total': len(tgt)})\n"
        "  elif isinstance(tgt,dict):\n"
        "    items=list(tgt.items())[:__lim]\n"
        "    _e({'kind':'object','columns':['key','value'],'data': [[k, (v if not isinstance(v,(dict,list)) else json.dumps(v))] for k,v in items]})\n"
        "  else:\n"
        "    _e({'kind':'scalar','value': tgt})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:JPREV]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/json/schema")
async def variable_json_schema(name: str, path: Optional[str] = None, limit: int = 1000, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    p = path or ""
    code = (
        "import json, collections\n"
        f"__n=r'''{name}'''\n"
        f"__path=r'''{p}'''\n"
        f"__lim=int({max(1,int(limit or 1000))})\n"
        "def _e(o): print('[[VAR:JSCHEMA]]'+json.dumps(o))\n"
        "def _parse_json(v):\n"
        "  try:\n"
        "    if isinstance(v,(dict,list)): return v\n"
        "    s=v.decode('utf-8','ignore') if isinstance(v,(bytes,bytearray)) else str(v)\n"
        "    return json.loads(s)\n"
        "  except Exception: return None\n"
        "def _nav(obj, path):\n"
        "  if not path: return obj\n"
        "  cur=obj\n"
        "  for tok in path.split('.'):\n"
        "    if tok=='': continue\n"
        "    if '[' in tok and tok.endswith(']'):\n"
        "      key, idx = tok.split('[',1)[0], tok.split('[',1)[1][:-1]\n"
        "      if key: cur = (cur.get(key) if isinstance(cur, dict) else None)\n"
        "      try: idx=int(idx)\n"
        "      except Exception: return None\n"
        "      try: cur = cur[idx]\n"
        "      except Exception: return None\n"
        "    else:\n"
        "      cur = cur.get(tok) if isinstance(cur, dict) else None\n"
        "    if cur is None: return None\n"
        "  return cur\n"
        "v=globals().get(__n,None)\n"
        "obj=_parse_json(v)\n"
        "if obj is None: _e({'error':'not json'})\n"
        "else:\n"
        "  tgt=_nav(obj,__path)\n"
        "  if tgt is None: _e({'error':'path not found'})\n"
        "  elif isinstance(tgt,list):\n"
        "    rows=tgt[:__lim]\n"
        "    counter=collections.defaultdict(set)\n"
        "    for r in rows:\n"
        "      if isinstance(r,dict):\n"
        "        for k,v in r.items():\n"
        "          counter[str(k)].add(type(v).__name__)\n"
        "    fields=[{'name':k,'types':sorted(list(v))} for k,v in counter.items()]\n"
        "    _e({'kind':'list<object>','fields':fields, 'sampleCount': len(rows)})\n"
        "  elif isinstance(tgt,dict):\n"
        "    fields=[{'name':str(k),'type': type(v).__name__} for k,v in tgt.items()]\n"
        "    _e({'kind':'object','fields':fields})\n"
        "  else:\n"
        "    _e({'kind':'scalar','type': type(tgt).__name__})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:JSCHEMA]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/xml/preview")
async def variable_xml_preview(name: str, xpath: Optional[str] = None, limit: int = 50, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    xp = xpath or '.'
    code = (
        "import json, xml.etree.ElementTree as ET\n"
        f"__n=r'''{name}'''\n"
        f"__xp=r'''{xp}'''\n"
        f"__lim=int({max(1,int(limit or 50))})\n"
        "def _e(o): print('[[VAR:XMLPREV]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    s = v.decode('utf-8','ignore') if isinstance(v,(bytes,bytearray)) else str(v)\n"
        "    root=ET.fromstring(s)\n"
        "    nodes = root.findall(__xp) if __xp else [root]\n"
        "    nodes = nodes[:__lim]\n"
        "    cols=set()\n"
        "    rows=[]\n"
        "    # collect union of attribute names and direct child tags\n"
        "    for el in nodes:\n"
        "      cols.update(el.attrib.keys())\n"
        "      for ch in list(el): cols.add('child:'+ch.tag)\n"
        "    cols=list(cols)[:100]\n"
        "    for el in nodes:\n"
        "      row=[]\n"
        "      for c in cols:\n"
        "        if c.startswith('child:'):\n"
        "          tag=c.split(':',1)[1]\n"
        "          ch=el.find(tag)\n"
        "          row.append(None if ch is None else (ch.text or ''))\n"
        "        else:\n"
        "          row.append(el.attrib.get(c))\n"
        "      rows.append(row)\n"
        "    _e({'columns': cols, 'data': rows, 'matched': len(nodes)})\n"
        "  except Exception as e:\n"
        "    _e({'error':'parse-failed','message': str(e)})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:XMLPREV]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/xml/tags")
async def variable_xml_tags(name: str, limit: int = 200, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json, xml.etree.ElementTree as ET, collections\n"
        f"__n=r'''{name}'''\n"
        f"__lim=int({max(1,int(limit or 200))})\n"
        "def _e(o): print('[[VAR:XMLTAGS]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    s = v.decode('utf-8','ignore') if isinstance(v,(bytes,bytearray)) else str(v)\n"
        "    root=ET.fromstring(s)\n"
        "    cnt=collections.Counter()\n"
        "    for el in root.iter(): cnt[el.tag]+=1\n"
        "    items=[[k,int(v)] for k,v in cnt.most_common(__lim)]\n"
        "    _e({'tags': items})\n"
        "  except Exception as e:\n"
        "    _e({'error':'parse-failed','message': str(e)})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:XMLTAGS]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/columns")
async def variable_columns(name: str, pattern: Optional[str] = None, limit: int = 200, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    pat = pattern or ""
    code = (
        "import json, re\n"
        f"__n=r'''{name}'''\n"
        f"__p=r'''{pat}'''\n"
        f"__lim=int({max(1,int(limit or 200))})\n"
        "def _e(o): print('[[VAR:COLS]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v,pd.DataFrame):\n"
        "      cols=[str(c) for c in v.columns]\n"
        "      if __p:\n"
        "        rgx=re.compile(__p)\n"
        "        cols=[c for c in cols if rgx.search(c)]\n"
        "      out={'columns': cols[:__lim], 'dtypes': {str(c): str(v[c].dtype) for c in v.columns if not __p or re.compile(__p).search(str(c))}}\n"
        "      _e(out)\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:COLS]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/value_counts")
async def variable_value_counts(name: str, column: str, limit: int = 20, dropna: bool = True, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        f"__n=r'''{name}'''\n"
        f"__c=r'''{column}'''\n"
        f"__lim=int({max(1,int(limit or 20))})\n"
        f"__dropna={bool(dropna)}\n"
        "def _e(o): print('[[VAR:VCOUNT]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v,pd.DataFrame) and __c in v.columns:\n"
        "      vc=v[__c].value_counts(dropna=__dropna).head(__lim)\n"
        "      _e({'column':__c,'items':[[str(k), int(v)] for k,v in vc.items()]})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:VCOUNT]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/unique")
async def variable_unique(name: str, column: str, limit: int = 100, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        f"__n=r'''{name}'''\n"
        f"__c=r'''{column}'''\n"
        f"__lim=int({max(1,int(limit or 100))})\n"
        "def _e(o): print('[[VAR:UNIQ]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v,pd.DataFrame) and __c in v.columns:\n"
        "      u=v[__c].drop_duplicates().head(__lim).tolist()\n"
        "      _e({'column':__c,'values':u})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:UNIQ]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/describe")
async def variable_describe(name: str, columns: Optional[str] = None, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    cols = columns or ""
    code = (
        "import json\n"
        f"__n=r'''{name}'''\n"
        f"__cols=r'''{cols}'''\n"
        "def _e(o): print('[[VAR:DESC]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v,pd.DataFrame):\n"
        "      target=v\n"
        "      if __cols.strip():\n"
        "        cs=[c.strip() for c in __cols.split(',') if c.strip() and c in v.columns]\n"
        "        if cs: target=v[cs]\n"
        "      try:\n"
        "        desc=target.describe(include='all').to_dict()\n"
        "      except Exception:\n"
        "        desc={}\n"
        "      _e({'columns': list(target.columns), 'describe': desc})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:DESC]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/histogram")
async def variable_histogram(name: str, column: str, bins: int = 20, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json, math\n"
        f"__n=r'''{name}'''\n"
        f"__c=r'''{column}'''\n"
        f"__b=int({max(1,int(bins or 20))})\n"
        "def _e(o): print('[[VAR:HIST]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd, numpy as np\n"
        "    if isinstance(v,pd.DataFrame) and __c in v.columns:\n"
        "      s=pd.to_numeric(v[__c], errors='coerce').dropna()\n"
        "      if len(s)==0: _e({'error':'no numeric data'})\n"
        "      else:\n"
        "        cts, edges = np.histogram(s.values, bins=__b)\n"
        "        _e({'column':__c, 'counts': cts.tolist(), 'edges': edges.tolist()})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:HIST]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/rows")
async def variable_rows(name: str, offset: int = 0, limit: int = 100, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    code = (
        "import json\n"
        f"__n=r'''{name}'''\n"
        f"__off=int({max(0,int(offset or 0))})\n"
        f"__lim=int({max(1,int(limit or 100))})\n"
        "def _e(o): print('[[VAR:ROWS]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v,pd.DataFrame):\n"
        "      sl = v.iloc[__off:__off+__lim]\n"
        "      _e({'columns':[str(c) for c in sl.columns], 'data': sl.to_numpy().tolist(), 'offset': __off, 'limit': __lim, 'total': int(len(v))})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:ROWS]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.get("/api/variables/{name}/corr")
async def variable_corr(name: str, columns: Optional[str] = None, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    cols = columns or ""
    code = (
        "import json\n"
        f"__n=r'''{name}'''\n"
        f"__cols=r'''{cols}'''\n"
        "def _e(o): print('[[VAR:CORR]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "else:\n"
        "  try:\n"
        "    import pandas as pd\n"
        "    if isinstance(v,pd.DataFrame):\n"
        "      target=v\n"
        "      if __cols.strip():\n"
        "        cs=[c.strip() for c in __cols.split(',') if c.strip() and c in v.columns]\n"
        "        if cs: target=v[cs]\n"
        "      try: m=target.corr(numeric_only=True)\n"
        "      except Exception: m=None\n"
        "      _e({'columns': list(m.columns) if m is not None else [], 'matrix': m.values.tolist() if m is not None else []})\n"
        "    else: _e({'error':'unsupported'})\n"
        "  except Exception: _e({'error':'unsupported'})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:CORR]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}


@app.post("/api/variables/{name}/filter")
async def variable_filter(name: str, body: dict, _: bool = Depends(require_auth)):
    """Apply simple filters to a DataFrame safely.
    Body example:
    { "filters": [{"column":"species","op":"eq","value":"setosa"}], "limit": 50, "sort_by":"sepal_length", "descending": false }
    Supported ops: eq, ne, lt, gt, le, ge, contains, isin
    Returns: { columns, data, total_before, total_after }
    """
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    filters = body.get("filters") or []
    limit = int(body.get("limit") or 100)
    sort_by = body.get("sort_by")
    desc = bool(body.get("descending") or False)
    # Pre-validate ops to a whitelist
    ops = {"eq","ne","lt","gt","le","ge","contains","isin"}
    safe_filters = []
    for f in filters:
        c = str(f.get("column",""))
        op = str(f.get("op",""))
        if not c or op not in ops:
            continue
        val = f.get("value", None)
        if op == "isin" and not isinstance(val, list):
            continue
        safe_filters.append({"column": c, "op": op, "value": val})
    import json as _json  # local for f-string safety
    fl_json = _json.dumps(safe_filters)
    sort_js = json.dumps(sort_by) if sort_by is not None else "null"
    code = (
        "import json, pandas as pd\n"
        f"__n=r'''{name}'''\n"
        f"__filters=json.loads(r'''{fl_json}''')\n"
        f"__limit=int({max(1,limit)})\n"
        f"__sort={sort_js}\n"
        f"__desc={desc}\n"
        "def _e(o): print('[[VAR:FILTER]]'+json.dumps(o))\n"
        "v=globals().get(__n,None)\n"
        "if v is None: _e({'error':'not found'})\n"
        "elif not isinstance(v, pd.DataFrame): _e({'error':'unsupported'})\n"
        "else:\n"
        "  df=v\n"
        "  total_before=int(len(df))\n"
        "  for f in __filters:\n"
        "    c=f.get('column'); op=f.get('op'); val=f.get('value')\n"
        "    if c not in df.columns: continue\n"
        "    try:\n"
        "      if op=='eq': df=df[df[c]==val]\n"
        "      elif op=='ne': df=df[df[c]!=val]\n"
        "      elif op=='lt': df=df[df[c]<val]\n"
        "      elif op=='gt': df=df[df[c]>val]\n"
        "      elif op=='le': df=df[df[c]<=val]\n"
        "      elif op=='ge': df=df[df[c]>=val]\n"
        "      elif op=='contains': df=df[df[c].astype(str).str.contains(str(val), na=False, case=False)]\n"
        "      elif op=='isin': df=df[df[c].isin(val)]\n"
        "    except Exception: pass\n"
        "  if __sort and __sort in df.columns:\n"
        "    try: df=df.sort_values(by=__sort, ascending=(not __desc))\n"
        "    except Exception: pass\n"
        "  total_after=int(len(df))\n"
        "  sl=df.head(__limit)\n"
        "  _e({'columns':[str(c) for c in sl.columns], 'data': sl.to_numpy().tolist(), 'total_before': total_before, 'total_after': total_after})\n"
    )
    async with iopub_gate:
        msg_id = kc.execute(code)
        data = _await_json("[[VAR:FILTER]]", msg_id)
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return data or {"error": "timeout"}
