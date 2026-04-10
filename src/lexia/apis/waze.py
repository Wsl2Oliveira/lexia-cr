"""Find Shard — Waze API (CPF → shard mapping)."""
from __future__ import annotations

import structlog

from lexia.apis.auth import get_authenticated_client, get_uber_token

log = structlog.get_logger(__name__)

WAZE_URL = "https://prod-global-waze.nubank.com.br/api/mapping/cpf"


async def find_shard(cpf: str) -> str | None:
    """Look up which shard a CPF belongs to.

    Returns the shard identifier (e.g. 's0', 's1') or None.
    """
    token = await get_uber_token()
    async with get_authenticated_client(token) as client:
        resp = await client.post(WAZE_URL, json={"tax_id": cpf})
        resp.raise_for_status()
        data = resp.json()

    shard = data.get("shard")
    log.info("shard_found", cpf=cpf[:3] + "***", shard=shard)
    return shard
