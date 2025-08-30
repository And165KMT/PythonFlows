from pathlib import Path
from typing import List, Dict, Any
import json
import re
import os


_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _root_dir() -> Path:
    # backend/flows.py -> project root is parent of backend
    here = Path(__file__).resolve()
    root = here.parent.parent
    return root


def flows_dir() -> Path:
    # Allow override via env var
    base = os.environ.get("PYFLOWS_DATA_DIR")
    root = Path(base) if base else _root_dir() / "data" / "flows"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sanitize_name(name: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError("invalid flow name; use [A-Za-z0-9_-], up to 64 chars")
    return name


def list_flows() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    d = flows_dir()
    for p in d.glob("*.json"):
        try:
            st = p.stat()
            out.append({
                "name": p.stem,
                "mtime": int(st.st_mtime),
                "size": int(st.st_size),
                "path": str(p)
            })
        except Exception:
            continue
    out.sort(key=lambda x: x["name"].lower())
    return out


def save_flow(name: str, data: Dict[str, Any]) -> Path:
    nm = _sanitize_name(name)
    p = flows_dir() / f"{nm}.json"
    # add minimal version marker if absent
    if isinstance(data, dict) and "version" not in data:
        data = { **data, "version": 1 }
    tmp = p.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(p)
    return p


def load_flow(name: str) -> Dict[str, Any]:
    nm = _sanitize_name(name)
    p = flows_dir() / f"{nm}.json"
    if not p.exists():
        raise FileNotFoundError(nm)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def delete_flow(name: str) -> bool:
    nm = _sanitize_name(name)
    p = flows_dir() / f"{nm}.json"
    if p.exists():
        p.unlink()
        return True
    return False
