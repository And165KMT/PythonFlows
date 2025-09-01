import asyncio
import subprocess
import time
import queue
from typing import Optional, Dict
from jupyter_client.manager import KernelManager


# Global kernel state shared across routers
kernel_manager: Optional[KernelManager] = None
kc = None  # type: ignore

# Coordinate exclusive iopub reads
iopub_gate = asyncio.Lock()

# In-flight pip installs (id -> subprocess.Popen)
pip_processes: Dict[str, subprocess.Popen] = {}


async def start_new_kernel():
    """Start a fresh Jupyter kernel, replacing any existing one."""
    global kernel_manager, kc
    if kernel_manager:
        try:
            kernel_manager.shutdown_kernel(now=True)
        except Exception:
            pass
        kernel_manager = None
    km = KernelManager()
    km.start_kernel()
    c = km.client()
    c.start_channels()
    try:
        c.wait_for_ready(timeout=10)
    except Exception:
        pass
    kernel_manager = km
    kc = c

    # Prime the kernel with FlowPython helper prelude so that generated code
    # (and ad-hoc snippets) can rely on these helpers being present.
    try:
        prelude = "\n".join([
            "import importlib",
            "__pf_imports = globals().get('__pf_imports', {})",
            "def _fp_register_import(mod, alias=None):",
            "    try:",
            "        m = importlib.import_module(str(mod))",
            "        ver = getattr(m, '__version__', None)",
            "    except Exception:",
            "        ver = None",
            "    d = globals().get('__pf_imports', {})",
            "    d[str(mod)] = {'alias': alias, 'version': (str(ver) if ver is not None else None)}",
            "    globals()['__pf_imports'] = d",
            "def _fp_env():",
            "    _safe_builtins = {'abs': abs, 'round': round, 'min': min, 'max': max, 'pow': pow}",
            "    import math as _m",
            "    return {'__builtins__': _safe_builtins, 'math': _m, 'PI': _m.pi}",
            "def _fp_set_globals(text):",
            "    lines = str(text).splitlines()",
            "    env = _fp_env()",
            "    for __ln in lines:",
            "        __ln = __ln.strip()",
            "        if not __ln or __ln.startswith('#'): continue",
            "        __name, __eq, __expr = __ln.partition('=')",
            "        __name = __name.strip(); __expr = __expr.strip()",
            "        if not __name or not __expr: continue",
            "        try:",
            "            globals()[__name] = eval(__expr, env, globals())",
            "        except Exception:",
            "            try:",
            "                exec(f\"{__name} = (\" + __expr + \")\", globals())",
            "            except Exception:",
            "                pass",
            "    rows = []",
            "    for __ln in lines:",
            "        __name, __eq, __expr = __ln.partition('=')",
            "        __name = __name.strip()",
            "        if not __name: continue",
            "        try:",
            "            __val = globals().get(__name, None)",
            "            rows.append({'name': __name, 'type': type(__val).__name__, 'repr': repr(__val)[:200]})",
            "        except Exception:",
            "            rows.append({'name': __name, 'type': 'unknown', 'repr': '<unrepr>'})",
            "    return rows",
            "def _fp_as_scalar(x):",
            "    try:",
            "        # numpy scalar (optional)",
            "        try:",
            "            import numpy as _np",
            "            if isinstance(x, _np.ndarray):",
            "                try: return x.item()",
            "                except Exception: pass",
            "        except Exception:",
            "            pass",
            "        if isinstance(x, list) and len(x)==1:",
            "            e = x[0]",
            "            if isinstance(e, dict):",
            "                if 'text' in e: return e.get('text')",
            "                if len(e)==1: return next(iter(e.values()))",
            "            return e",
            "        if isinstance(x, dict):",
            "            if 'text' in x and isinstance(x['text'], (str, bytes)): return x['text']",
            "            if len(x)==1: return next(iter(x.values()))",
            "        if isinstance(x, (list, tuple)) and len(x)==1:",
            "            return x[0]",
            "        return x",
            "    except Exception:",
            "        return x",
            "def _fp_preview(x, nid):",
            "    try:",
            "        s = f'type={type(x).__name__}'",
            "        print(f'[[PREVIEW:{nid}:HEAD]]' + str(x))",
            "        print(f'[[PREVIEW:{nid}:HEADHTML]]<pre>' + str(x) + '</pre>')",
            "        print(f'[[PREVIEW:{nid}:DESC]]' + s)",
            "        print(f'[[PREVIEW:{nid}:DESCHTML]]<pre>' + s + '</pre>')",
            "    except Exception:",
            "        pass",
        ])
        async with iopub_gate:
            msg_id = kc.execute(prelude)
            # Drain until idle or timeout
            deadline = time.time() + 4.0
            try:
                while time.time() < deadline:
                    try:
                        msg = kc.get_iopub_msg(timeout=0.2)
                    except queue.Empty:
                        continue
                    if msg.get('parent_header', {}).get('msg_id') != msg_id:
                        continue
                    mtype = msg.get('header', {}).get('msg_type')
                    if mtype == 'status' and msg.get('content', {}).get('execution_state') == 'idle':
                        break
            except Exception:
                pass
    except Exception:
        # Prelude is best-effort; ignore failures
        pass


async def shutdown_kernel():
    """Stop channels and shutdown kernel if present."""
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


def get_kc():  # Optional[Any]
    return kc


def get_kernel_manager():
    return kernel_manager
