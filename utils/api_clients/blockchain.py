# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Extracted Blockchain.com charts API interaction logic from
#   blockchain_onchain_pipeline.py into a reusable client module.
#
# WHY:
#   - Shared between daily ingestion and backfill_controller.
#   - Chart-name ↔ metric-name mapping and point parsing in one place.
# ---------------------------------------------------------------------------
from __future__ import annotations

import json
from datetime import datetime, timezone

import requests

from utils.validators.payload import safe_float, safe_int

CHARTS: dict[str, str] = {
    "btc_tx_count": "n-transactions",
    "btc_unique_addresses": "n-unique-addresses",
    "btc_hash_rate": "hash-rate",
    "btc_mempool_size": "mempool-size",
}


def fetch_chart_points(chart_name: str, timespan_days: int, timeout: int = 30) -> tuple[dict, dict]:
    """Fetch a single Blockchain.com chart and return ``(raw_payload, {date: value})``."""
    url = f"https://api.blockchain.info/charts/{chart_name}"
    response = requests.get(
        url,
        params={"timespan": f"{timespan_days}days", "format": "json", "sampled": "true"},
        timeout=timeout,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Blockchain.com chart request failed for {chart_name} "
            f"with status {response.status_code}: {response.text[:500]}"
        )

    payload = response.json()
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, list):
        raise RuntimeError(f"Blockchain.com payload for {chart_name} is missing a values array")

    points: dict = {}
    for item in values:
        if not isinstance(item, dict):
            continue
        timestamp = safe_int(item.get("x"))
        point_value = safe_float(item.get("y"))
        if timestamp is None or point_value is None:
            continue
        metric_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        points[metric_date] = point_value

    return payload, points


def fetch_onchain_metrics(timespan_days: int = 120) -> tuple[list[tuple], dict]:
    """Fetch all on-chain charts and return ``(records, payload_cache)``.

    Each record is a tuple matching the bronze ``onchain_macro_raw`` schema:
    ``(metric_date, btc_tx_count, btc_unique_addresses, btc_hash_rate,
      btc_mempool_size, raw_json, ingestion_ts, source, run_key)``
    — but **without** ingestion_ts / source / run_key (caller fills those).

    Returns ``(combined_by_date_dict, payload_cache)``.
    """
    combined_by_date: dict = {}
    payload_cache: dict = {}

    for metric_name, chart_name in CHARTS.items():
        payload, points = fetch_chart_points(chart_name, timespan_days)
        payload_cache[metric_name] = payload
        for metric_date, metric_value in points.items():
            combined_by_date.setdefault(metric_date, {})[metric_name] = metric_value

    if not combined_by_date:
        raise RuntimeError("No on-chain rows were returned by Blockchain.com charts API")

    return combined_by_date, payload_cache
