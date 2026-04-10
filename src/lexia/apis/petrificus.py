"""Find Blocks (freeze orders) — Petrificus-Parcialus API."""
from __future__ import annotations

import structlog

from lexia.apis.auth import get_authenticated_client, get_uber_token

log = structlog.get_logger(__name__)


async def find_blocks(customer_id: str, shard: str) -> list[dict]:
    """Fetch freeze/block orders for a customer.

    Returns a list of freeze-order dicts with type, amount, status, etc.
    """
    url = (
        f"https://prod-{shard}-petrificus-parcialus.nubank.com.br"
        f"/api/customers/{customer_id}/freeze-orders"
    )
    token = await get_uber_token()

    async with get_authenticated_client(token) as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            log.info("no_blocks", customer_id=customer_id[:8])
            return []
        resp.raise_for_status()
        data = resp.json()

    blocks = data if isinstance(data, list) else data.get("freeze_orders", [])
    log.info("blocks_found", customer_id=customer_id[:8], count=len(blocks))
    return blocks
