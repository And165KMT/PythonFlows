import asyncio
import subprocess
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
