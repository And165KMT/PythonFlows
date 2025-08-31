import json
import queue
import time
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from ..auth import require_auth
from ..config import kernel_feature_enabled
from ..exec_control import exec_registry
from ..kernel_runtime import get_kc, iopub_gate
from .introspect import _introspect_module_code


router = APIRouter()


def _curation_for(module: str):
    """Placeholder for future curation rules. Returns tuple (include, exclude, limit, category)."""
    return None


@router.post("/api/autogen/test")
async def api_autogen_test(body: dict, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return JSONResponse({"error": "kernel feature disabled"}, status_code=403)
    kc = get_kc()
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    module = str(body.get("module") or "").strip()
    if not module:
        return JSONResponse({"error": "missing 'module'"}, status_code=400)
    include = str(body.get("include") or "")
    exclude = str(body.get("exclude") or r"^_")
    try:
        limit = int(body.get("limit") or 50)
    except Exception:
        limit = 50
    try:
        max_tests = int(body.get("maxTests") or 10)
    except Exception:
        max_tests = 10
    cur = _curation_for(module)
    inc2, exc2, lim2, cat2 = (cur or (None, None, None, None))
    eff_inc = inc2 or include
    eff_exc = exc2 or exclude
    eff_lim = int(lim2 or limit)
    code = _introspect_module_code(module, eff_inc, eff_exc, eff_lim)
    specs = []
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
                            specs = (data or {}).get("nodes") or []
                        except Exception:
                            specs = []
                        break
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
        except Exception:
            pass
    try:
        await exec_registry.resolve(msg_id)
    except Exception:
        pass
    items = []
    for spec in (specs[:max_tests] if isinstance(specs, list) else []):
        sid = str(spec.get("id") or "")
        title = str(spec.get("title") or sid)
        call = spec.get("call") or {}
        target = str(call.get("target") or "")
        kind = str(call.get("kind") or "function")
        dfp = call.get("dfParam")
        py = []
        py.append("import json")
        root = target.split(".")[0] if target else (module.split(".")[0] if module else "")
        if root:
            py.append(f"import {root}")
            py.append("def _fp_register_import(mod, alias=None):\n    try:\n        d = globals().get('__pf_imports', {})\n        d[mod] = {'alias': alias, 'version': None}\n        globals()['__pf_imports'] = d\n    except Exception: pass")
            py.append(f"_fp_register_import('{root}')")
        py.append("try:\n import pandas as pd\nexcept Exception:\n pd=None")
        py.append("try:\n import numpy as np\nexcept Exception:\n np=None")
        py.append("df = None\nX=None\ny=None\ntry:\n df = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]}) if pd is not None else None\nexcept Exception: df=None")
        py.append("try:\n import numpy as _np\n X = _np.array([[1,2],[3,4],[5,6]])\n y = _np.array([0,1,0])\nexcept Exception:\n X=None; y=None")
        if kind == "constructor":
            py.append(f"_ok=True; _err=None\ntry:\n obj = {target}()\nexcept Exception as e:\n _ok=False; _err=str(e)[:200]")
        elif kind == "method":
            recv = target.rsplit(".",1)[0]
            meth = target.rsplit(".",1)[1]
            py.append("_ok=True; _err=None")
            py.append("try:\n obj = None\n try:\n  obj = " + recv + "()\n except Exception:\n  obj = None\n if obj is not None:\n  try:\n   if 'fit'== '" + meth + "': obj.fit(X, y)\n   elif 'predict'== '" + meth + "': obj.fit(X, y); _ = obj.predict(X)\n   else: getattr(obj, '" + meth + "')(X)\n  except Exception as e:\n   _ok=False; _err=str(e)[:200]\nelse:\n _ok=False; _err='ctor failed'")
        else:
            if dfp:
                py.append(f"_ok=True; _err=None\ntry:\n _ = {target}({dfp}=df if df is not None else X)\nexcept Exception as e:\n _ok=False; _err=str(e)[:200]")
            else:
                py.append(f"_ok=True; _err=None\ntry:\n _ = {target}()\nexcept Exception as e:\n _ok=False; _err=str(e)[:200]")
        py.append(f"print('[[TEST]]'+json.dumps({{'id': r'''{sid}''', 'title': r'''{title}''', 'kind': r'''{kind}''', 'ok': bool(_ok), 'error': (_err if not _ok else None)}}))")
        payload = None
        async with iopub_gate:
            msg_id2 = kc.execute("\n".join(py))
            deadline = time.time() + 6.0
            try:
                while time.time() < deadline:
                    try:
                        msg = kc.get_iopub_msg(timeout=0.2)
                    except queue.Empty:
                        continue
                    if msg.get("parent_header", {}).get("msg_id") != msg_id2:
                        continue
                    mtype = msg.get("header", {}).get("msg_type")
                    content = msg.get("content", {})
                    if mtype == "stream":
                        text = content.get("text", "")
                        if text.startswith("[[TEST]]"):
                            try:
                                payload = json.loads(text[len("[[TEST]]"):])
                            except Exception:
                                payload = None
                            break
                    elif mtype == "status" and content.get("execution_state") == "idle":
                        break
            except Exception:
                pass
        try:
            await exec_registry.resolve(msg_id2)
        except Exception:
            pass
        if isinstance(payload, dict):
            items.append(payload)
        else:
            items.append({"id": sid, "title": title, "kind": kind, "ok": False, "error": "no result"})
    return {"items": items}
