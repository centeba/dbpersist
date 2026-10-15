"""Static API-key authentication shared by both services."""

from fastapi import Header, HTTPException, Request, status


async def require_api_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
    expected: str = request.app.state.settings.api_key
    if x_api_key != expected:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")
