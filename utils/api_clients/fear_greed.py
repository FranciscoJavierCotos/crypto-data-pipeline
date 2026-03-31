# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Extracted Fear & Greed Index API interaction logic from
#   fear_greed_pipeline.py into a reusable client module.
#
# WHY:
#   - Shared between daily ingestion and backfill_controller.
#   - Validation of the API response in one place.
# ---------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime, timezone

import requests

from utils.validators.payload import safe_int


def fetch_fear_greed_index(timeout: int = 30) -> dict:
    """Call Alternative.me Fear & Greed API and return a validated data point.

    Returns a dict with keys: value, value_classification, metric_date,
    time_until_update.
    """
    url = "https://api.alternative.me/fng/?limit=1&format=json"
    response = requests.get(url, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"Alternative.me request failed with status {response.status_code}: {response.text[:500]}"
        )

    raw_text = response.text
    payload = response.json()

    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    metadata_error = metadata.get("error") if isinstance(metadata, dict) else None
    if metadata_error is not None:
        raise RuntimeError(f"Alternative.me returned metadata.error: {metadata_error}")

    data_points = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_points, list) or not data_points:
        raise RuntimeError("Alternative.me payload is missing a non-empty data array")

    latest = data_points[0]
    if not isinstance(latest, dict):
        raise RuntimeError("Alternative.me payload data[0] is not a JSON object")

    value = safe_int(latest.get("value"))
    value_classification = latest.get("value_classification")
    metric_timestamp = safe_int(latest.get("timestamp"))
    metric_date = (
        datetime.fromtimestamp(metric_timestamp, tz=timezone.utc).date()
        if metric_timestamp is not None
        else None
    )
    time_until_update = safe_int(latest.get("time_until_update"))

    if value is None:
        raise RuntimeError("Alternative.me payload is missing a valid integer 'value'")
    if value_classification is None:
        raise RuntimeError("Alternative.me payload is missing 'value_classification'")
    if metric_date is None:
        raise RuntimeError("Alternative.me payload is missing a valid Unix 'timestamp'")

    return {
        "value": value,
        "value_classification": value_classification,
        "metric_date": metric_date,
        "time_until_update": time_until_update,
        "raw_json": raw_text,
    }
