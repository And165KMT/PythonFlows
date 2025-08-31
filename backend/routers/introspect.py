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


def _introspect_module_code(mod: str, include: str, exclude: str, limit: int) -> str:
    return (
        "import json, importlib, inspect, re\n"
        f"_modname = r'''{mod}'''\n"
        f"_inc = re.compile(r'''{include or '.*'}''')\n"
        f"_exc = re.compile(r'''{exclude or '^_'}''')\n"
        f"_limit = int({max(1, int(limit or 50))})\n"
        "_out = []\n"
        "def _short_doc(obj):\n"
        "  try:\n"
        "    d = inspect.getdoc(obj)\n"
        "    if not d: return ''\n"
        "    line = str(d).strip().splitlines()[0] if str(d).strip().splitlines() else ''\n"
        "    return line[:180]\n"
        "  except Exception:\n"
        "    return ''\n"
        "def _param_specs(obj):\n"
        "  try:\n"
        "    sig = inspect.signature(obj)\n"
        "  except Exception:\n"
        "    return []\n"
        "  ps = []\n"
        "  for n, p in sig.parameters.items():\n"
        "    if n in ('self','cls'): continue\n"
        "    if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD): continue\n"
        "    d_val = None\n"
        "    if p.default is not inspect._empty:\n"
        "      try:\n"
        "        d_val = p.default if isinstance(p.default,(str,int,float,bool)) else str(p.default)\n"
        "      except Exception:\n"
        "        d_val = str(p.default)\n"
        "    ps.append({'name': n, 'default': d_val, 'ui': 'string'})\n"
        "  return ps\n"
        "try:\n"
        "  M = importlib.import_module(_modname)\n"
        "  _root = _modname.split('.')[0] if _modname else 'autogen'\n"
        "  _cat = (_modname.split('.')[-1] if '.' in _modname else _root).capitalize()\n"
        "  for name, obj in inspect.getmembers(M):\n"
        "    if not _inc.search(name): continue\n"
        "    if _exc.search(name): continue\n"
        "    try:\n"
        "      if inspect.isfunction(obj):\n"
        "        params = _param_specs(obj)\n"
        "        _out.append({\n"
        "          'id': f'autogen.{_modname}.{name}',\n"
        "          'title': name,\n"
        "          'desc': _short_doc(obj),\n"
        "          'category': _cat,\n"
        "          'inputType': 'Any',\n"
        "          'outputType': 'Any',\n"
        "          'params': params,\n"
        "          'pkg': _root,\n"
        "          'call': { 'target': f'{_modname}.{name}', 'kind': 'function', 'receiver': None, 'dfParam': (params[0]['name'] if (isinstance(params,list) and len(params)>0 and isinstance(params[0],dict) and 'name' in params[0]) else None), 'srcParams': ([p['name'] for p in params][:2] if isinstance(params,list) else []), 'returnsSelf': False }\n"
        "        })\n"
        "      elif inspect.isclass(obj):\n"
        "        params = _param_specs(obj)\n"
        "        _out.append({\n"
        "          'id': f'autogen.{_modname}.{name}',\n"
        "          'title': name,\n"
        "          'desc': _short_doc(obj),\n"
        "          'category': 'Estimator' if hasattr(obj, 'fit') else _cat,\n"
        "          'inputType': 'Any',\n"
        "          'outputType': 'Estimator' if hasattr(obj, 'fit') else 'Any',\n"
        "          'params': params,\n"
        "          'pkg': _root,\n"
        "          'call': { 'target': f'{_modname}.{name}', 'kind': 'constructor', 'receiver': None, 'dfParam': None, 'srcParams': [], 'returnsSelf': False }\n"
        "        })\n"
        "        if hasattr(obj, 'fit'):\n"
        "          _out.append({\n"
        "            'id': f'autogen.{_modname}.{name}.fit',\n"
        "            'title': f'{name}.fit',\n"
        "            'desc': _short_doc(getattr(obj,'fit', None)),\n"
        "            'category': 'Estimator',\n"
        "            'inputType': 'Any',\n"
        "            'outputType': 'Estimator',\n"
        "            'params': _param_specs(getattr(obj,'fit', None)) if hasattr(obj,'fit') else [],\n"
        "            'pkg': _root,\n"
        "            'call': { 'target': f'{_modname}.{name}.fit', 'kind': 'method', 'receiver': 'estimator', 'dfParam': 'X', 'srcParams': ['X','y'], 'returnsSelf': True }\n"
        "          })\n"
        "        if hasattr(obj, 'predict'):\n"
        "          _out.append({\n"
        "            'id': f'autogen.{_modname}.{name}.predict',\n"
        "            'title': f'{name}.predict',\n"
        "            'desc': _short_doc(getattr(obj,'predict', None)),\n"
        "            'category': 'Estimator',\n"
        "            'inputType': 'Any',\n"
        "            'outputType': 'Any',\n"
        "            'params': _param_specs(getattr(obj,'predict', None)) if hasattr(obj,'predict') else [],\n"
        "            'pkg': _root,\n"
        "            'call': { 'target': f'{_modname}.{name}.predict', 'kind': 'method', 'receiver': 'estimator', 'dfParam': 'X', 'srcParams': ['X'], 'returnsSelf': False }\n"
        "          })\n"
        "    except Exception:\n"
        "      pass\n"
        "  _out = _out[:_limit]\n"
        "  print('[[INTROSPECT]]' + json.dumps({'nodes': _out}))\n"
        "except Exception as e:\n"
        "  print('[[INTROSPECT]]' + json.dumps({'nodes': []}))\n"
    )


@router.get("/api/introspect_module")
async def api_introspect_module(module: str, include: Optional[str] = None, exclude: Optional[str] = r"^_", limit: int = 50, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"nodes": []}
    kc = get_kc()
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
    inc = include or ""
    exc = exclude or r"^_"
    lim = int(limit or 50)
    code = _introspect_module_code(module, inc, exc, lim)
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


@router.get("/api/introspect")
async def api_introspect_target(target: str, _: bool = Depends(require_auth)):
    if not kernel_feature_enabled():
        return {"nodes": []}
    kc = get_kc()
    if kc is None:
        return JSONResponse({"error": "kernel not ready"}, status_code=503)
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
        "    if hasattr(obj, 'fit'): out.append({'id': f'autogen.{modpath}.{cname}.fit', 'title': f'{cname}.fit', 'category': 'Estimator', 'inputType':'Any', 'outputType':'Estimator', 'params': _param_specs(getattr(obj,'fit', None)), 'pkg': root, 'call': {'target': f'{modpath}.{cname}.fit', 'kind':'method', 'receiver': 'estimator', 'dfParam': 'X', 'srcParams': ['X','y'], 'returnsSelf': True }}\n"
        "    if hasattr(obj, 'predict'): out.append({'id': f'autogen.{modpath}.{cname}.predict', 'title': f'{cname}.predict', 'category': 'Estimator', 'inputType':'Any', 'outputType':'Any', 'params': _param_specs(getattr(obj,'predict', None)), 'pkg': root, 'call': {'target': f'{modpath}.{cname}.predict', 'kind':'method', 'receiver': 'estimator', 'dfParam': 'X', 'srcParams': ['X'], 'returnsSelf': False }}\n"
        "  elif inspect.isfunction(obj):\n"
        "    fname = obj.__name__\n"
        "    _cat = (modpath.split('.')[-1] if modpath else root).capitalize()\n"
        "    out.append({'id': f'autogen.{modpath}.{fname}', 'title': fname, 'category': _cat, 'inputType':'Any', 'outputType':'Any', 'params': _param_specs(obj), 'pkg': root, 'call': {'target': _tgt, 'kind':'function', 'receiver': None, 'dfParam': None, 'returnsSelf': False}})\n"
        "  _emit(out)\n"
        "except Exception:\n"
        "  _emit([])\n"
    )
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
