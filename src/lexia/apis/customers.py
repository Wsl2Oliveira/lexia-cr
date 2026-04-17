"""Find Customer ID — Customers API (tax ID → customer ID)."""

from __future__ import annotations

import structlog

from lexia.apis.auth import get_authenticated_client, get_uber_token

log = structlog.get_logger(__name__)


async def find_customer_id(cpf: str, shard: str) -> str | None:
    """Find the Nubank customer ID by tax ID (CPF/CNPJ).

    Returns customer_id or None if not a customer.
    """
    url = f"https://prod-{shard}-customers.nubank.com.br/api/customers/person/find-by-tax-id"
    token = await get_uber_token()

    async with get_authenticated_client(token) as client:
        resp = await client.post(url, json={"tax_id": cpf})

        if resp.status_code == 404:
            log.info("customer_not_found", cpf=cpf[:3] + "***")
            return None

        resp.raise_for_status()
        data = resp.json()

    customer_id = data.get("id") or data.get("customer_id")
    log.info("customer_found", customer_id=customer_id[:8] + "..." if customer_id else None)
    return customer_id
