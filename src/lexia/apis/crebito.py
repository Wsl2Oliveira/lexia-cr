"""Find Active Cards — Crebito API."""
from __future__ import annotations

import structlog

from lexia.apis.auth import get_authenticated_client, get_uber_token

log = structlog.get_logger(__name__)


async def find_active_cards(customer_id: str, shard: str) -> list[dict]:
    """Fetch all non-canceled cards for a customer.

    Returns a list of card dicts with status, last_four_digits, etc.
    """
    url = (
        f"https://prod-{shard}-crebito.nubank.com.br"
        f"/api/credit-accounts/{customer_id}/cards/all-non-canceled"
    )
    token = await get_uber_token()

    async with get_authenticated_client(token) as client:
        resp = await client.get(url)
        if resp.status_code == 404:
            log.info("no_credit_account", customer_id=customer_id[:8])
            return []
        resp.raise_for_status()
        data = resp.json()

    cards = data if isinstance(data, list) else data.get("cards", [])
    log.info("cards_found", customer_id=customer_id[:8], count=len(cards))
    return cards
