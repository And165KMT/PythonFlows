import os


def verify_license_for_feature(feature: str, key: str | None) -> bool:
    """
    Placeholder license verification.
    - Returns True only if PYFLOWS_LICENSE_ALLOW='1' (for development/testing),
      otherwise False. Replace with real verification later.
    """
    if os.environ.get("PYFLOWS_LICENSE_ALLOW") == "1":
        return True
    return False
