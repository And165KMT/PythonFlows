import json
import queue
import time
from typing import Optional
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from ..auth import require_auth
from ..config import kernel_feature_enabled
from ..exec_control import exec_registry
from ..kernel_runtime import get_kc, iopub_gate


router = APIRouter()


@router.get("/api/modules/versions")
async def api_module_versions(names: str = "", _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"items": []}
    kc = get_kc()
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    raw = str(names or "").strip()
    mods = [s.strip() for s in raw.split(',') if s.strip()]
    if not mods:
        return {"items": []}
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


@router.get("/api/imports")
async def api_list_imports(_: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"items": []}
    kc = get_kc()
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


@router.get("/api/modules/installed")
async def api_modules_installed(q: Optional[str] = None, limit: int = 200, offset: int = 0, importableOnly: bool = False, _: bool = Depends(require_auth)):
    try:
        import importlib.metadata as _md
    except Exception:
        try:
            import importlib_metadata as _md  # type: ignore
        except Exception:
            _md = None  # type: ignore
    items = []
    if _md is not None:
        try:
            for dist in _md.distributions():
                try:
                    nm = dist.metadata.get('Name') or dist._name  # type: ignore[attr-defined]
                    ver = dist.version
                    if not nm:
                        continue
                    name = str(nm)
                    if q and q.strip():
                        if q.strip().lower() not in name.lower():
                            continue
                    items.append({"name": name, "version": str(ver)})
                except Exception:
                    continue
        except Exception:
            items = []
    if importableOnly and items:
        out = []
        for it in items:
            try:
                import importlib
                root = (it.get('name') or '').split('[')[0]
                if root:
                    importlib.import_module(root)
                    out.append(it)
            except Exception:
                continue
        items = out
    total = len(items)
    start = max(0, int(offset or 0))
    end = max(start, min(total, start + max(1, int(limit or 200))))
    page = items[start:end]
    return {"items": page, "total": total, "offset": start, "limit": end - start}
