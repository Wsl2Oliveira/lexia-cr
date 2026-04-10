"""Find Available Assets — Rayquaza API."""
from __future__ import annotations

import structlog

from lexia.apis.auth import get_authenticated_client, get_uber_token

log = structlog.get_logger(__name__)


async def find_available_assets(customer_id: str, shard: str) -> list[dict]:
    """Fetch available assets (balances) for a customer.

    Returns a list of asset dicts with type, amount, currency, etc.
    """
    url = (
        f"https://prod-{shard}-rayquaza.nubank.com.br"
        f"/api/customers/{customer_id}/available-assets"
    )
    token = await get_uber_token()

    async with get_authenticated_client(token) as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            log.info("no_assets", customer_id=customer_id[:8])
            return []
        resp.raise_for_status()
        data = resp.json()

    assets = data if isinstance(data, list) else data.get("assets", [])
    log.info("assets_found", customer_id=customer_id[:8], count=len(assets))
    return assets
