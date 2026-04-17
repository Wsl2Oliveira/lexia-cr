"""Nubank internal API authentication — certificate-based + uber token."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx
import structlog

from lexia.config import settings

log = structlog.get_logger(__name__)

_cached_token: _AuthToken | None = None


@dataclass
class _AuthToken:
    access_token: str
    expires_at: datetime


def _build_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.load_cert_chain(
        certfile=settings.nu_cert_path,
        keyfile=settings.nu_cert_key_path,
    )
    return ctx


async def get_uber_token() -> str:
    """Obtain (or reuse cached) Nubank uber token via certificate auth."""
    global _cached_token

    if _cached_token and datetime.utcnow() < _cached_token.expires_at:
        return _cached_token.access_token

    ssl_ctx = _build_ssl_context()

    async with httpx.AsyncClient(verify=ssl_ctx) as client:
        resp = await client.post(
            settings.nu_auth_url,
            json={"grant_type": "client_credentials"},
        )
        resp.raise_for_status()
        data = resp.json()

    token = data["access_token"]
    expires_in = data.get("expires_in", 3600)
    _cached_token = _AuthToken(
        access_token=token,
        expires_at=datetime.utcnow() + timedelta(seconds=expires_in - 60),
    )

    log.info("uber_token_obtained", expires_in=expires_in)
    return token


def get_authenticated_client(token: str) -> httpx.AsyncClient:
    """Return an httpx client with Nubank auth headers and client certificate."""
    ssl_ctx = _build_ssl_context()
    return httpx.AsyncClient(
        verify=ssl_ctx,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    )
