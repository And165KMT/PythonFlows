import asyncio
import json
import queue
import time
from typing import Optional
from fastapi import APIRouter, Depends, Form
from fastapi.responses import JSONResponse
from ..auth import require_auth
from ..config import kernel_feature_enabled, variables_list_timeout_seconds
from ..exec_control import exec_registry
from ..kernel_runtime import get_kc, iopub_gate


router = APIRouter()


@router.get("/api/variables")
async def list_variables(pattern: Optional[str] = None, include_private: bool = False, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"variables": []}
    kc = get_kc()
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    pattern_js = pattern or ""
    include_private_flag = "True" if include_private else "False"
    code = (
        "import json, types, re\n"
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
        "        out.append({'name': str(k), 'type': t, 'repr': r})\n"
        "    print('[[VARS]]' + json.dumps(out))\n"
        "__fp_list_vars()\n"
    )
    data = None
    async with iopub_gate:
        msg_id = kc.execute(code)
        deadline = time.time() + variables_list_timeout_seconds()
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
                if mtype == "stream" and content.get("text", "").startswith("[[VARS]]"):
                    payload = content.get("text", "")[8:]
                    try:
                        data = json.loads(payload)
                    except Exception:
                        data = []
                    break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    return {"variables": data or []}


@router.delete("/api/variables/{name}")
async def variable_delete(name: str, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    kc = get_kc()
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
                if mtype == "stream" and content.get("text", "").startswith("[[VAR:DEL]]"):
                    result = content.get("text", "").split("]]",1)[-1]
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


@router.post("/api/variables/{name}/rename")
async def variable_rename(name: str, to: Optional[str] = Form(None), body: Optional[dict] = None, _: bool = Depends(require_auth)):
    if body and not to:
        to = body.get("to")  # type: ignore
    if not to:
        return JSONResponse({"error": "missing 'to'"}, status_code=400)
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    kc = get_kc()
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
                if mtype == "stream" and content.get("text", "").startswith("[[VAR:REN]]"):
                    res = content.get("text", "")[len("[[VAR:REN]]"):]
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
