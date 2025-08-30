import os
from .license import verify_license_for_feature


def _truthy(val: str | None) -> bool:
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
