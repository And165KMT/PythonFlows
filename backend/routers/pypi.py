from typing import Optional
from fastapi import APIRouter, Depends
from ..auth import require_auth
import urllib.request as _urlreq
import urllib.parse as _urlparse
from html import unescape as _unesc
import re


router = APIRouter()


@router.get("/api/pypi/search")
async def api_pypi_search(q: Optional[str] = None, limit: int = 20, offset: int = 0, _: bool = Depends(require_auth)):
    query = (q or "").strip()
    if not query:
        return {"items": [], "total": 0}
    try:
        limit = max(1, min(50, int(limit)))
    except Exception:
        limit = 20
    try:
        offset = max(0, int(offset))
    except Exception:
        offset = 0
    items: list = []
    total = 0
    last_err: Optional[str] = None
    try:
        page = max(1, int(offset // max(1, limit) + 1))
        url = f"https://pypi.org/search/?q={_urlparse.quote(query)}&page={page}"
        req = _urlreq.Request(url, headers={"User-Agent": "FlowPython/1.0"})
        with _urlreq.urlopen(req, timeout=8) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        entries = []
        for m in re.finditer(r'<a\s+class=\"package-snippet\"[^>]*>(.*?)</a>', html, re.S | re.I):
            try:
                inner = m.group(1)
                name = ""
                ver = ""
                s1 = re.search(r'<span[^>]*class=\"package-snippet__name\"[^>]*>(.*?)</span>', inner, re.S | re.I)
                if s1:
                    name = _unesc(re.sub(r"<.*?>", "", s1.group(1))).strip() or name
                s2 = re.search(r'<span[^>]*class=\"package-snippet__version\"[^>]*>(.*?)</span>', inner, re.S | re.I)
                if s2:
                    ver = _unesc(re.sub(r"<.*?>", "", s2.group(1))).strip() or ver
                s3 = re.search(r'<p[^>]*class=\"package-snippet__description\"[^>]*>(.*?)</p>', inner, re.S | re.I)
                summary = _unesc(re.sub(r"<.*?>", "", (s3.group(1) if s3 else "")).strip())
                if name:
                    entries.append({"name": name, "version": ver, "summary": summary})
            except Exception:
                continue
        total = len(entries)
        items = entries[:limit]
        if not items:
            try:
                url2 = f"https://pypi.org/pypi/{_urlparse.quote(query)}/json"
                req2 = _urlreq.Request(url2, headers={"User-Agent": "FlowPython/1.0"})
                with _urlreq.urlopen(req2, timeout=8) as resp2:
                    import json as _json
                    data = _json.loads(resp2.read().decode("utf-8", errors="ignore"))
                info = data.get("info", {}) if isinstance(data, dict) else {}
                name = str(info.get("name") or query)
                ver = str(info.get("version") or "")
                summary = str(info.get("summary") or "")
                if name:
                    items = [{"name": name, "version": ver, "summary": summary}]
                    total = 1
            except Exception:
                pass
    except Exception as e:
        last_err = str(e)
        items = []
    return {"items": items or [], "total": total, "error": None if items else (last_err or None)}
