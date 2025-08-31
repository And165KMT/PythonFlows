from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from pathlib import Path as _Path
import os
from .config import kernel_feature_enabled
from .kernel_runtime import start_new_kernel, shutdown_kernel
from .routers import base as base_router
from .routers import flows as flows_router
from .routers import kernel as kernel_router
from .routers import variables as variables_router
from .routers import modules as modules_router
from .routers import pip_install as pip_router
from .routers import pypi as pypi_router
from .routers import introspect as introspect_router
from .routers import autogen as autogen_router
from .routers import packages as packages_router


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static frontend dir
static_dir = Path(__file__).resolve().parent.parent / "frontend"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    # Serve JS packages (dynamic node definitions) under /pkg/<name>/<entry>
    pkg_dir = static_dir / "packages"
    if pkg_dir.exists():
        app.mount("/pkg", StaticFiles(directory=str(pkg_dir)), name="pkg")


# Routers
app.include_router(base_router.router)
app.include_router(flows_router.router)
app.include_router(kernel_router.router)
app.include_router(variables_router.router)
app.include_router(modules_router.router)
app.include_router(pip_router.router)
app.include_router(pypi_router.router)
app.include_router(introspect_router.router)
app.include_router(autogen_router.router)
app.include_router(packages_router.router)


@app.on_event("startup")
async def _startup():
    # Load .env if present
    try:
        from dotenv import load_dotenv  # type: ignore
        root_dir = _Path(__file__).resolve().parent.parent
        load_dotenv(dotenv_path=root_dir / ".env")
    except Exception:
        pass
    if kernel_feature_enabled():
        await start_new_kernel()


@app.on_event("shutdown")
async def _shutdown():
    await shutdown_kernel()