from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException, Query, Request, WebSocket, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from utils.config import settings


class ErrorResponse(BaseModel):
    detail: str


COMMON_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Invalid request window or timeframe"},
    401: {"model": ErrorResponse, "description": "Invalid or missing bearer token"},
    404: {"model": ErrorResponse, "description": "Unknown symbol"},
}

_bearer = HTTPBearer(auto_error=False)


async def verify_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    token: str | None = Query(default=None, include_in_schema=False),
) -> None:
    if not settings.api_token:
        return
    supplied = credentials.credentials if credentials is not None else token
    if supplied != settings.api_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing bearer token"
        )


def verify_websocket_auth(websocket: WebSocket) -> bool:
    if not settings.api_token:
        return True
    header = websocket.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip() == settings.api_token
    return websocket.query_params.get("token") == settings.api_token


def get_clickhouse(request: Request) -> Any:
    client = getattr(request.app.state, "clickhouse", None)
    if client is None:
        raise HTTPException(status_code=503, detail="ClickHouse is not ready")
    return client


def get_redis(request: Request) -> Any:
    client = getattr(request.app.state, "redis", None)
    if client is None:
        raise HTTPException(status_code=503, detail="Redis is not ready")
    return client


def get_supervisor(request: Request) -> Any:
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="Workers are not ready")
    return supervisor
