# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Extracted shared payload validation and type-coercion helpers that were
#   duplicated across every ingestion DAG into a single shared module.
#
# WHY:
#   - Removes ~40 lines of duplicated safe_float / safe_int / timestamp
#     parsing from each pipeline file.
#   - Centralises validation rules so bug-fixes apply everywhere.
# ---------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_iso8601_timestamp(value) -> Optional[datetime]:
    """Parse an ISO-8601 string (or datetime) to a UTC-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def validate_coingecko_payload(data: list, required_columns: list[str]) -> None:
    """Validate that *data* is a non-empty list of dicts containing *required_columns*."""
    if not isinstance(data, list):
        raise ValueError("CoinGecko payload must be a list of coin records")
    if not data:
        raise ValueError("CoinGecko payload is empty")

    missing_columns: set[str] = set()
    for index, row in enumerate(data):
        if not isinstance(row, dict):
            raise ValueError(f"CoinGecko payload row {index} is not a JSON object")
        for column in required_columns:
            if column not in row:
                missing_columns.add(column)

    if missing_columns:
        raise ValueError(
            "CoinGecko payload is missing expected fields: " + ", ".join(sorted(missing_columns))
        )
