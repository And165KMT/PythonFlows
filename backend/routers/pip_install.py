import subprocess
import sys
import json
import uuid
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from ..auth import require_auth
from ..config import pip_timeout_seconds
from ..kernel_runtime import pip_processes


router = APIRouter()


@router.post("/api/pip/install")
async def api_pip_install(body: dict, _: bool = Depends(require_auth)):
    name = str(body.get("name") or "").strip()
    version = str(body.get("version") or "").strip()
    extras = str(body.get("extras") or "").strip()
    index_url = str(body.get("indexUrl") or "").strip()
    upgrade = bool(body.get("upgrade") or False)
    if not name:
        return JSONResponse({"error": "missing 'name'"}, status_code=400)
    req = name
    if extras:
        req += extras
    if version:
        if any(op in version for op in ["<",">","=","~",","]):
            req += version
        else:
            req += f"=={version}"
    args = [sys.executable, "-m", "pip", "install"]
    if upgrade:
        args.append("--upgrade")
    if index_url:
        args += ["-i", index_url]
    args.append(req)
    try:
        out = subprocess.check_output(args, stderr=subprocess.STDOUT, text=True, timeout=pip_timeout_seconds())
        modname = name.split("[")[0]
        ver = None
        try:
            import importlib
            m = importlib.import_module(modname)
            ver = getattr(m, "__version__", None)
        except Exception:
            ver = None
        return {"ok": True, "name": modname, "version": (str(ver) if ver is not None else None), "output": out[-2000:]}
    except subprocess.TimeoutExpired as e:
        return JSONResponse({"ok": False, "name": name, "error": "pip timeout", "output": (e.output or "")[-2000:]}, status_code=504)
    except subprocess.CalledProcessError as e:
        return JSONResponse({"ok": False, "name": name, "error": "pip failed", "output": (e.output or "")[-2000:]}, status_code=500)


@router.post("/api/pip/install/stream")
async def api_pip_install_stream(body: dict, _: bool = Depends(require_auth)):
    name = str(body.get("name") or "").strip()
    version = str(body.get("version") or "").strip()
    extras = str(body.get("extras") or "").strip()
    index_url = str(body.get("indexUrl") or "").strip()
    upgrade = bool(body.get("upgrade") or False)
    if not name:
        return JSONResponse({"error": "missing 'name'"}, status_code=400)
    req = name
    if extras:
        req += extras
    if version:
        if any(op in version for op in ["<",">","=","~",","]):
            req += version
        else:
            req += f"=={version}"
    args = [sys.executable, "-m", "pip", "install"]
    if upgrade:
        args.append("--upgrade")
    if index_url:
        args += ["-i", index_url]
    args.append(req)
    install_id = str(uuid.uuid4())
    timeout = pip_timeout_seconds()

    def _run():
        import time as _t
        try:
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        except Exception as e:
            yield f"ERROR: failed to start pip: {e}\n"
            return
        pip_processes[install_id] = proc
        start = _t.time()
        yield f"[[ID]] {install_id}\n"
        try:
            if proc.stdout is not None:
                for line in proc.stdout:
                    yield line if line.endswith('\n') else (line + '\n')
                    if _t.time() - start > timeout:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                        yield f"ERROR: timeout after {timeout}s\n"
                        break
        except Exception:
            pass
        rc = None
        try:
            rc = proc.wait(timeout=5)
        except Exception:
            rc = None
        modname = name.split("[")[0]
        ver = None
        if rc == 0:
            try:
                import importlib
                m = importlib.import_module(modname)
                ver = getattr(m, "__version__", None)
            except Exception:
                ver = None
        try:
            pip_processes.pop(install_id, None)
        except Exception:
            pass
        done = json.dumps({"name": modname, "version": (str(ver) if ver is not None else None), "rc": rc})
        yield f"[[DONE]] {done}\n"

    headers = {"X-Install-Id": install_id, "X-Accel-Buffering": "no"}
    return StreamingResponse(_run(), media_type="text/plain", headers=headers)


@router.post("/api/pip/cancel")
async def api_pip_cancel(body: dict, _: bool = Depends(require_auth)):
    iid = str(body.get("id") or "").strip()
    if not iid:
        return JSONResponse({"error": "missing 'id'"}, status_code=400)
    proc = pip_processes.get(iid)
    if not proc:
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        proc.kill()
        pip_processes.pop(iid, None)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
