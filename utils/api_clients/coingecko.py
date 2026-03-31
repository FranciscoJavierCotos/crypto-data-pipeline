# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Extracted CoinGecko API interaction logic from coingecko_pipeline.py into
#   a reusable client module.
#
# WHY:
#   - Shared between daily ingestion and backfill_controller.
#   - Header construction (demo vs pro key) in one place.
#   - Timeout and retry behaviour centralised.
# ---------------------------------------------------------------------------
from __future__ import annotations

import os

import requests

from utils.validators.payload import validate_coingecko_payload


def coingecko_headers() -> dict[str, str]:
    """Build CoinGecko auth headers from environment variables."""
    shared_api_key = (os.getenv("COINGECKO_API_KEY") or "").strip()
    demo_api_key = (os.getenv("COINGECKO_DEMO_API_KEY") or "").strip()
    pro_api_key = (os.getenv("COINGECKO_PRO_API_KEY") or "").strip()
    api_key_header = (os.getenv("COINGECKO_API_KEY_HEADER") or "demo").strip().lower()

    if pro_api_key:
        return {"x-cg-pro-api-key": pro_api_key}
    if demo_api_key:
        return {"x-cg-demo-api-key": demo_api_key}
    if shared_api_key:
        if api_key_header == "pro":
            return {"x-cg-pro-api-key": shared_api_key}
        return {"x-cg-demo-api-key": shared_api_key}
    return {}


COINGECKO_REQUIRED_COLUMNS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "last_updated",
]


def fetch_coingecko_market_data(
    vs_currency: str = "usd",
    per_page: int = 50,
    page: int = 1,
    timeout: int = 30,
) -> list[dict]:
    """Call CoinGecko /coins/markets and return validated payload."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    headers = coingecko_headers()
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": False,
    }

    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    validate_coingecko_payload(data, COINGECKO_REQUIRED_COLUMNS)
    return data
