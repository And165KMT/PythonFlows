from fastapi import Depends, Header, HTTPException, status, WebSocket
from typing import Optional
from .config import get_api_token, auth_required


def _token_from_header(authorization: Optional[str], x_api_token: Optional[str]) -> Optional[str]:
    if authorization:
        # Authorization: Bearer <token>
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == 'bearer':
            return parts[1]
    if x_api_token:
        return x_api_token
    return None


def require_auth(authorization: Optional[str] = Header(None), x_api_token: Optional[str] = Header(None)):
    """FastAPI dependency for HTTP endpoints. No-op if auth is not required."""
    if not auth_required():
        return True
    want = get_api_token()
    got = _token_from_header(authorization, x_api_token)
    if not want or not got or want != got:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or missing token")
    return True


async def require_ws_auth(ws: WebSocket):
    """Check WebSocket auth. No-op if auth is not required."""
    if not auth_required():
        return True
    want = get_api_token()
    if not want:
        return True
    # Try header first
    auth_header = ws.headers.get('authorization')
    token = None
    if auth_header:
        parts = auth_header.split()
        if len(parts) == 2 and parts[0].lower() == 'bearer':
            token = parts[1]
    # Fallback to query string ?token=
    if not token:
        token = ws.query_params.get('token')
    if token != want:
        await ws.close(code=4401)
        return False
    return True
