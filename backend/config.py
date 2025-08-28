import os
from typing import Optional
from .license import verify_license_for_feature


def _truthy(val: Optional[str]) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


def kernel_feature_enabled() -> bool:
    """
    Decide whether the Jupyter kernel feature is enabled.
    Priority:
    1) PYFLOWS_DISABLE_KERNEL=true -> False
    2) PYFLOWS_ENABLE_KERNEL=true -> True
    3) PYFLOWS_LICENSE_KEY validates for feature 'kernel' -> True
    4) Else False
    """
    if _truthy(os.environ.get("PYFLOWS_DISABLE_KERNEL")):
        return False
    if _truthy(os.environ.get("PYFLOWS_ENABLE_KERNEL")):
        return True
    key = os.environ.get("PYFLOWS_LICENSE_KEY")
    if key and verify_license_for_feature("kernel", key):
        return True
    return False


# --- New configuration helpers (backward compatible) ---
def get_api_token() -> Optional[str]:
    """Return API token if set; when set, auth becomes required for protected endpoints."""
    tok = os.environ.get("PYFLOWS_API_TOKEN")
    return tok.strip() if tok else None


def auth_required() -> bool:
    return bool(get_api_token())


def exec_timeout_seconds() -> int:
    """Max seconds to allow a /run execution before sending an interrupt (0 = disabled)."""
    try:
        v = int(os.environ.get("PYFLOWS_EXEC_TIMEOUT", "0").strip())
        return max(0, v)
    except Exception:
        return 0


def export_max_rows_default() -> int:
    """Default max rows for CSV export if not specified by query."""
    try:
        v = int(os.environ.get("PYFLOWS_EXPORT_MAX_ROWS", "200000").strip())
        return max(1, v)
    except Exception:
        return 200000


def timeout_restart_enabled() -> bool:
    """If true, after interrupting on timeout we will restart the kernel."""
    return _truthy(os.environ.get("PYFLOWS_TIMEOUT_RESTART"))


# --- UI/IOPub wait timeouts ---
def variables_list_timeout_seconds() -> float:
    """Max seconds to wait while collecting variable list output from the kernel."""
    try:
        v = float(os.environ.get("PYFLOWS_VARS_TIMEOUT", "3.0").strip())
        return max(0.1, v)
    except Exception:
        return 3.0


def export_timeout_seconds() -> float:
    """Max seconds to wait while exporting a variable from the kernel."""
    try:
        v = float(os.environ.get("PYFLOWS_EXPORT_TIMEOUT", "5.0").strip())
        return max(0.1, v)
    except Exception:
        return 5.0


def upload_max_mb_default() -> int:
    """Maximum upload size in MB (per file). 0 or negative disables the limit."""
    try:
        v = int(os.environ.get("PYFLOWS_UPLOAD_MAX_MB", "0").strip())
        return v
    except Exception:
        return 0
