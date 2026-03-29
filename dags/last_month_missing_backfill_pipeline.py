from datetime import date, datetime, time as dt_time, timedelta, timezone
import json
import os
import time
from urllib.parse import urlparse

import pendulum
import requests
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.bases.hook import BaseHook
from databricks import sql

from pipeline_callbacks import task_failure_alert


CHARTS = {
    "btc_tx_count": "n-transactions",
    "btc_unique_addresses": "n-unique-addresses",
    "btc_hash_rate": "hash-rate",
    "btc_mempool_size": "mempool-size",
}


def _quote_identifier(identifier):
    if identifier is None:
        raise ValueError("Identifier cannot be None")
    value = str(identifier).strip()
    if not value:
        raise ValueError("Identifier cannot be empty")
    return f"`{value.replace('`', '``')}`"


def _qualified_table_name(catalog, schema, table):
    return ".".join(
        [
            _quote_identifier(catalog),
            _quote_identifier(schema),
            _quote_identifier(table),
        ]
    )


def _qualified_schema_name(catalog, schema):
    return ".".join([_quote_identifier(catalog), _quote_identifier(schema)])


def _chunk_records(records, batch_size):
    for start in range(0, len(records), batch_size):
        yield records[start : start + batch_size]


def _insert_records_in_batches(cursor, table_name, records, column_names, batch_size=200):
    if not records:
        return

    row_placeholder = "(" + ", ".join(["?"] * len(column_names)) + ")"
    insert_columns_sql = ", ".join(column_names)

    for batch in _chunk_records(records, batch_size):
        values_sql = ", ".join([row_placeholder] * len(batch))
        insert_sql = f"""
            INSERT INTO {table_name} ({insert_columns_sql})
            VALUES {values_sql}
        """
        flat_params = [value for row in batch for value in row]
        cursor.execute(insert_sql, flat_params)


def _ensure_columns_exist(cursor, table_name, columns_with_types):
    for column_name, column_type in columns_with_types:
        try:
            cursor.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMNS ({column_name} {column_type})
                """
            )
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso8601_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        normalized_value = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized_value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _extract_usd_value(data, key):
    if not isinstance(data, dict):
        return None
    field = data.get(key)
    if isinstance(field, dict):
        return field.get("usd")
    return None


def _extract_usd_timestamp(data, key):
    if not isinstance(data, dict):
        return None
    field = data.get(key)
    if isinstance(field, dict):
        return _parse_iso8601_timestamp(field.get("usd"))
    return None


def _target_day_close_utc(target_date):
    return datetime.combine(target_date, dt_time(23, 59, 59), tzinfo=timezone.utc)


def _build_run_key(prefix, target_date):
    dag_run_id = (os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "").strip()
    if dag_run_id:
        return f"{prefix}:{target_date.isoformat()}:{dag_run_id}"
    suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{prefix}:{target_date.isoformat()}:manual:{suffix}"


def _get_databricks_connection_params():
    conn = BaseHook.get_connection("databricks_default")
    extras = conn.extra_dejson or {}

    def _clean(value):
        if isinstance(value, str):
            return value.strip()
        return value

    def _is_placeholder(value):
        if not value:
            return False
        value_lower = value.lower()
        return (
            "replace_me" in value_lower
            or "replace-me" in value_lower
            or "<" in value
            or ">" in value
            or "databricks_pat" in value_lower
            or "your_" in value_lower
            or value_lower in {"changeme", "change-me", "placeholder"}
        )

    host = _clean(conn.host or extras.get("host") or os.getenv("DATABRICKS_HOST"))
    token = _clean(conn.password or extras.get("token") or os.getenv("DATABRICKS_TOKEN"))
    http_path = _clean(extras.get("http_path") or os.getenv("DATABRICKS_HTTP_PATH"))

    if host and "://" in host:
        parsed_host = urlparse(host).hostname
        host = parsed_host or host

    if not host:
        raise ValueError("Missing Databricks host in connection 'databricks_default'")
    if not token:
        raise ValueError("Missing Databricks token in connection 'databricks_default'")
    if not http_path:
        raise ValueError(
            "Missing Databricks SQL warehouse http_path in connection extras or DATABRICKS_HTTP_PATH env"
        )
    if _is_placeholder(http_path):
        raise ValueError(
            "DATABRICKS_HTTP_PATH is still a placeholder. Set it to your real SQL warehouse path, e.g. /sql/1.0/warehouses/<warehouse_id>."
        )
    if _is_placeholder(token):
        raise ValueError(
            "Databricks token looks like a placeholder. Set a valid PAT in connection 'databricks_default' or DATABRICKS_TOKEN."
        )

    return {
        "host": host,
        "token": token,
        "http_path": http_path,
    }


def _connect_databricks():
    params = _get_databricks_connection_params()
    return sql.connect(
        server_hostname=params["host"],
        http_path=params["http_path"],
        access_token=params["token"],
    )


def _coingecko_headers():
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


def _coingecko_requests_per_minute():
    configured = _safe_int(os.getenv("COINGECKO_RATE_LIMIT_PER_MINUTE"))
    return max(1, configured or 30)


def _build_rate_limit_waiter(requests_per_minute):
    min_interval_seconds = 60.0 / max(1, requests_per_minute)
    next_allowed_at = 0.0

    def _wait_for_slot():
        nonlocal next_allowed_at
        now = time.monotonic()
        if now < next_allowed_at:
            time.sleep(next_allowed_at - now)
            now = time.monotonic()
        next_allowed_at = max(next_allowed_at, now) + min_interval_seconds

    return _wait_for_slot, min_interval_seconds


def _http_get_json_with_retries(
    url,
    params,
    timeout=30,
    retries=4,
    backoff_seconds=2.0,
    headers=None,
    swallow_http_statuses=None,
    rate_limit_waiter=None,
):
    last_error_text = None
    swallow_http_statuses = set(swallow_http_statuses or [])
    for attempt in range(1, retries + 1):
        if rate_limit_waiter:
            rate_limit_waiter()
        response = requests.get(url, params=params, timeout=timeout, headers=headers)

        if response.status_code in swallow_http_statuses:
            response_text = (response.text or "").strip().replace("\n", " ")
            response_text = response_text[:300] if response_text else "<empty response body>"
            print(
                f"Skipping HTTP {response.status_code} from {url}. "
                f"Response: {response_text}"
            )
            return None

        if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
            retry_after = response.headers.get("Retry-After")
            retry_after_seconds = _safe_float(retry_after)
            wait_seconds = (
                retry_after_seconds
                if retry_after_seconds is not None and retry_after_seconds > 0
                else backoff_seconds * attempt
            )
            print(
                f"Retryable HTTP {response.status_code} from {url}. "
                f"Retrying in {wait_seconds:.1f}s (attempt {attempt}/{retries})"
            )
            time.sleep(wait_seconds)
            continue

        if 400 <= response.status_code < 500:
            response_text = (response.text or "").strip().replace("\n", " ")
            response_text = response_text[:300] if response_text else "<empty response body>"
            raise RuntimeError(
                f"Non-retryable HTTP {response.status_code} from {url}. "
                f"Response: {response_text}"
            )

        try:
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error_text = str(exc)
            if attempt < retries:
                wait_seconds = backoff_seconds * attempt
                print(
                    f"HTTP request failed for {url}: {exc}. "
                    f"Retrying in {wait_seconds:.1f}s (attempt {attempt}/{retries})"
                )
                time.sleep(wait_seconds)
                continue
            break

    raise RuntimeError(f"HTTP request failed after {retries} attempts for {url}: {last_error_text}")


def _parse_conf_date(raw_value, parameter_name):
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, date) and not isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, str):
        try:
            return date.fromisoformat(raw_value.strip())
        except ValueError as exc:
            raise ValueError(
                f"Invalid {parameter_name} '{raw_value}'. Use ISO format YYYY-MM-DD."
            ) from exc
    raise ValueError(
        f"Unsupported {parameter_name} type '{type(raw_value).__name__}'. "
        "Use ISO format YYYY-MM-DD."
    )


def _resolve_window_dates(conf):
    madrid_now = pendulum.now("Europe/Madrid")
    default_end = madrid_now.subtract(days=1).date()
    default_start = madrid_now.subtract(months=1).date()

    start_date = _parse_conf_date(conf.get("backfill_start_date"), "backfill_start_date") or default_start
    end_date = _parse_conf_date(conf.get("backfill_end_date"), "backfill_end_date") or default_end

    if start_date > end_date:
        raise ValueError(
            f"backfill_start_date {start_date.isoformat()} is after backfill_end_date {end_date.isoformat()}"
        )
    if end_date >= madrid_now.date():
        raise ValueError(
            f"backfill_end_date must be before today in Europe/Madrid. Received {end_date.isoformat()}"
        )

    return start_date, end_date


def _date_range(start_date, end_date):
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def _extract_date_from_db(value):
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"Unsupported date value type from Databricks: {type(value).__name__}")


def _query_existing_dates(cursor, table_name, date_expression, source, start_date, end_date):
    query = f"""
        SELECT {date_expression} AS metric_date
        FROM {table_name}
        WHERE source = ?
          AND {date_expression} BETWEEN ? AND ?
        GROUP BY {date_expression}
    """
    try:
        cursor.execute(query, [source, start_date, end_date])
    except Exception as exc:
        error_text = str(exc).lower()
        if (
            "table_or_view_not_found" in error_text
            or "table or view not found" in error_text
            or "schema_not_found" in error_text
            or "schema not found" in error_text
            or "cannot resolve" in error_text
        ):
            return set()
        raise

    existing = set()
    for row in cursor.fetchall() or []:
        metric_date = _extract_date_from_db(row[0])
        if metric_date is not None:
            existing.add(metric_date)
    return existing


def _detect_missing_dates(start_date, end_date):
    all_dates = set(_date_range(start_date, end_date))

    coingecko_catalog = os.getenv("DATABRICKS_CATALOG", "main")
    common_schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    coingecko_table = os.getenv("DATABRICKS_TABLE", "coingecko_market_data")

    fear_greed_catalog = os.getenv("DATABRICKS_CATALOG", "crypto-data-pipeline")
    fear_greed_table = os.getenv("DATABRICKS_FEAR_GREED_TABLE", "fear_greed_raw")

    onchain_catalog = os.getenv("DATABRICKS_CATALOG", "main")
    onchain_table = os.getenv("DATABRICKS_ONCHAIN_TABLE", "onchain_macro_raw")

    with _connect_databricks() as connection:
        with connection.cursor() as cursor:
            coingecko_existing = _query_existing_dates(
                cursor,
                _qualified_table_name(coingecko_catalog, common_schema, coingecko_table),
                "CAST(last_updated AS DATE)",
                "coingecko_api",
                start_date,
                end_date,
            )
            fear_greed_existing = _query_existing_dates(
                cursor,
                _qualified_table_name(fear_greed_catalog, common_schema, fear_greed_table),
                "metric_date",
                "alternative_me_api",
                start_date,
                end_date,
            )
            onchain_existing = _query_existing_dates(
                cursor,
                _qualified_table_name(onchain_catalog, common_schema, onchain_table),
                "metric_date",
                "blockchain_info_charts_api",
                start_date,
                end_date,
            )

    return {
        "coingecko": sorted(all_dates - coingecko_existing),
        "fear_greed": sorted(all_dates - fear_greed_existing),
        "onchain": sorted(all_dates - onchain_existing),
    }


def _ingest_missing_coingecko_dates(missing_dates):
    if not missing_dates:
        return {
            "rows_written": 0,
            "missing_dates_count": 0,
            "ingested_dates": [],
        }

    headers = _coingecko_headers()
    has_api_key = bool(headers)
    requests_per_minute = _coingecko_requests_per_minute()
    rate_limit_waiter, min_interval_seconds = _build_rate_limit_waiter(requests_per_minute)
    default_request_delay_seconds = 0.0
    default_history_retries = 6 if has_api_key else 3
    default_top_n_coins = 50 if has_api_key else 20
    default_min_successful_rows = 20 if has_api_key else 10

    request_delay_seconds = (
        _safe_float(os.getenv("COINGECKO_BACKFILL_REQUEST_DELAY_SECONDS"))
        or default_request_delay_seconds
    )
    history_retries = _safe_int(os.getenv("COINGECKO_BACKFILL_HISTORY_RETRIES")) or default_history_retries
    top_n_coins = _safe_int(os.getenv("COINGECKO_BACKFILL_TOP_COINS")) or default_top_n_coins
    min_successful_rows = (
        _safe_int(os.getenv("COINGECKO_BACKFILL_MIN_SUCCESS_ROWS"))
        or default_min_successful_rows
    )

    list_url = "https://api.coingecko.com/api/v3/coins/markets"
    list_params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": min(top_n_coins, 250),
        "page": 1,
        "sparkline": False,
    }
    top_coins = _http_get_json_with_retries(
        list_url,
        params=list_params,
        timeout=30,
        retries=history_retries,
        backoff_seconds=2.0,
        headers=headers,
        rate_limit_waiter=rate_limit_waiter,
    )
    if not isinstance(top_coins, list) or not top_coins:
        raise RuntimeError("CoinGecko top coins response was empty")
    top_coins = top_coins[:top_n_coins]

    history_url_template = "https://api.coingecko.com/api/v3/coins/{coin_id}/history"

    all_records = []
    ingested_dates = []
    skipped_dates_no_rows = []
    partial_dates_below_threshold = []
    for target_date in missing_dates:
        target_date_text = target_date.strftime("%d-%m-%Y")
        ingestion_ts = datetime.now(timezone.utc)
        records_by_id = {}
        swallowed_history_responses = 0

        for coin in top_coins:
            coin_id = coin.get("id") if isinstance(coin, dict) else None
            if not coin_id:
                continue

            history_url = history_url_template.format(coin_id=coin_id)
            try:
                payload = _http_get_json_with_retries(
                    history_url,
                    params={
                        "date": target_date_text,
                        "localization": "false",
                    },
                    timeout=30,
                    retries=history_retries,
                    backoff_seconds=2.0,
                    headers=headers,
                    swallow_http_statuses={400, 404},
                    rate_limit_waiter=rate_limit_waiter,
                )
            except RuntimeError as exc:
                print(f"Skipping coin {coin_id} for {target_date.isoformat()} after retry exhaustion: {exc}")
                if request_delay_seconds > 0:
                    time.sleep(request_delay_seconds)
                continue

            if payload is None:
                swallowed_history_responses += 1
                if request_delay_seconds > 0:
                    time.sleep(request_delay_seconds)
                continue

            market_data = payload.get("market_data") if isinstance(payload, dict) else None
            current_price = _safe_float(_extract_usd_value(market_data, "current_price"))
            if current_price is None:
                if request_delay_seconds > 0:
                    time.sleep(request_delay_seconds)
                continue

            market_cap = _safe_float(_extract_usd_value(market_data, "market_cap"))
            total_volume = _safe_float(_extract_usd_value(market_data, "total_volume"))
            volume_to_market_cap_ratio = (
                (total_volume / market_cap)
                if market_cap not in (None, 0) and total_volume is not None
                else None
            )

            market_cap_rank = _safe_int(payload.get("market_cap_rank"))
            symbol = payload.get("symbol")
            name = payload.get("name")

            ath_date = _extract_usd_timestamp(market_data, "ath_date")
            atl_date = _extract_usd_timestamp(market_data, "atl_date")

            run_key = _build_run_key("backfill_last_month_coingecko", target_date)
            records_by_id[coin_id] = (
                coin_id,
                symbol,
                name,
                current_price,
                _safe_int(market_cap),
                _safe_int(total_volume),
                _safe_float(_extract_usd_value(market_data, "high_24h")),
                _safe_float(_extract_usd_value(market_data, "low_24h")),
                _safe_float(_extract_usd_value(market_data, "price_change_24h")),
                _safe_float(_extract_usd_value(market_data, "price_change_percentage_24h")),
                market_cap_rank,
                _safe_float(_extract_usd_value(market_data, "fully_diluted_valuation")),
                _safe_float(_extract_usd_value(market_data, "market_cap_change_24h")),
                _safe_float(_extract_usd_value(market_data, "market_cap_change_percentage_24h")),
                _safe_float(market_data.get("circulating_supply") if isinstance(market_data, dict) else None),
                _safe_float(market_data.get("total_supply") if isinstance(market_data, dict) else None),
                _safe_float(market_data.get("max_supply") if isinstance(market_data, dict) else None),
                _safe_float(_extract_usd_value(market_data, "ath")),
                _safe_float(_extract_usd_value(market_data, "ath_change_percentage")),
                ath_date,
                _safe_float(_extract_usd_value(market_data, "atl")),
                _safe_float(_extract_usd_value(market_data, "atl_change_percentage")),
                atl_date,
                volume_to_market_cap_ratio,
                _target_day_close_utc(target_date),
                json.dumps(payload),
                ingestion_ts,
                "coingecko_api",
                run_key,
            )

            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)

        date_records = list(records_by_id.values())
        if not date_records:
            skipped_dates_no_rows.append(target_date.isoformat())
            print(
                f"CoinGecko backfill for {target_date.isoformat()} produced 0 valid rows. "
                f"Skipping this date. History responses skipped via swallow rules: "
                f"{swallowed_history_responses}/{len(top_coins)}."
            )
            continue

        if len(date_records) < min_successful_rows:
            partial_dates_below_threshold.append(
                f"{target_date.isoformat()}:{len(date_records)}"
            )
            print(
                f"CoinGecko backfill for {target_date.isoformat()} produced {len(date_records)} rows, "
                f"below configured threshold {min_successful_rows}. "
                "Continuing and ingesting partial data for this date."
            )

        all_records.extend(date_records)
        ingested_dates.append(target_date)

    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_TABLE", "coingecko_market_data")
    schema_name = _qualified_schema_name(catalog, schema)
    table_name = _qualified_table_name(catalog, schema, table)

    print(
        "CoinGecko backfill throttle: "
        f"max {requests_per_minute} req/min "
        f"(~{min_interval_seconds:.2f}s min interval), "
        f"extra_delay_per_request={request_delay_seconds:.2f}s"
    )

    with _connect_databricks() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id STRING,
                    symbol STRING,
                    name STRING,
                    current_price DOUBLE,
                    market_cap BIGINT,
                    total_volume BIGINT,
                    high_24h DOUBLE,
                    low_24h DOUBLE,
                    price_change_24h DOUBLE,
                    price_change_percentage_24h DOUBLE,
                    market_cap_rank INT,
                    fully_diluted_valuation DOUBLE,
                    market_cap_change_24h DOUBLE,
                    market_cap_change_percentage_24h DOUBLE,
                    circulating_supply DOUBLE,
                    total_supply DOUBLE,
                    max_supply DOUBLE,
                    ath DOUBLE,
                    ath_change_percentage DOUBLE,
                    ath_date TIMESTAMP,
                    atl DOUBLE,
                    atl_change_percentage DOUBLE,
                    atl_date TIMESTAMP,
                    volume_to_market_cap_ratio DOUBLE,
                    last_updated TIMESTAMP,
                    raw_json STRING,
                    ingestion_ts TIMESTAMP,
                    source STRING,
                    run_key STRING
                )
                USING DELTA
                """
            )
            _ensure_columns_exist(
                cursor,
                table_name,
                [
                    ("market_cap_rank", "INT"),
                    ("fully_diluted_valuation", "DOUBLE"),
                    ("market_cap_change_24h", "DOUBLE"),
                    ("market_cap_change_percentage_24h", "DOUBLE"),
                    ("circulating_supply", "DOUBLE"),
                    ("total_supply", "DOUBLE"),
                    ("max_supply", "DOUBLE"),
                    ("ath", "DOUBLE"),
                    ("ath_change_percentage", "DOUBLE"),
                    ("ath_date", "TIMESTAMP"),
                    ("atl", "DOUBLE"),
                    ("atl_change_percentage", "DOUBLE"),
                    ("atl_date", "TIMESTAMP"),
                    ("volume_to_market_cap_ratio", "DOUBLE"),
                    ("raw_json", "STRING"),
                    ("ingestion_ts", "TIMESTAMP"),
                    ("source", "STRING"),
                    ("run_key", "STRING"),
                ],
            )

            if ingested_dates:
                date_placeholders = ", ".join(["?"] * len(ingested_dates))
                cursor.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE source = ?
                      AND CAST(last_updated AS DATE) IN ({date_placeholders})
                    """,
                    ["coingecko_api", *ingested_dates],
                )

            column_names = [
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
                "market_cap_rank",
                "fully_diluted_valuation",
                "market_cap_change_24h",
                "market_cap_change_percentage_24h",
                "circulating_supply",
                "total_supply",
                "max_supply",
                "ath",
                "ath_change_percentage",
                "ath_date",
                "atl",
                "atl_change_percentage",
                "atl_date",
                "volume_to_market_cap_ratio",
                "last_updated",
                "raw_json",
                "ingestion_ts",
                "source",
                "run_key",
            ]
            if all_records:
                _insert_records_in_batches(
                    cursor,
                    table_name,
                    all_records,
                    column_names,
                    batch_size=100,
                )
            else:
                print("No valid CoinGecko records found for requested missing dates. Skipping bronze insert.")

    return {
        "rows_written": len(all_records),
        "missing_dates_count": len(missing_dates),
        "ingested_dates": [d.isoformat() for d in ingested_dates],
        "skipped_dates_no_rows": skipped_dates_no_rows,
        "partial_dates_below_threshold": partial_dates_below_threshold,
    }


def _ingest_missing_fear_greed_dates(missing_dates):
    if not missing_dates:
        return {
            "rows_written": 0,
            "missing_dates_count": 0,
            "ingested_dates": [],
        }

    response = requests.get("https://api.alternative.me/fng/?limit=0&format=json", timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Alternative.me request failed with status {response.status_code}: {response.text[:500]}"
        )

    payload = response.json()
    data_points = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_points, list) or not data_points:
        raise RuntimeError("Alternative.me payload is missing a non-empty data array")

    points_by_date = {}
    for point in data_points:
        if not isinstance(point, dict):
            continue
        metric_timestamp = _safe_int(point.get("timestamp"))
        if metric_timestamp is None:
            continue
        metric_day = datetime.fromtimestamp(metric_timestamp, tz=timezone.utc).date()
        points_by_date[metric_day] = point

    ingestion_ts = datetime.now(timezone.utc)
    records = []
    missing_from_api = []
    ingested_dates = []
    for target_date in missing_dates:
        point = points_by_date.get(target_date)
        if not point:
            missing_from_api.append(target_date.isoformat())
            continue

        value = _safe_int(point.get("value"))
        value_classification = point.get("value_classification")
        if value is None or not value_classification:
            missing_from_api.append(target_date.isoformat())
            continue

        run_key = _build_run_key("backfill_last_month_fear_greed", target_date)
        records.append(
            (
                value,
                value_classification,
                target_date,
                _safe_int(point.get("time_until_update")),
                json.dumps(point),
                ingestion_ts,
                "alternative_me_api",
                run_key,
            )
        )
        ingested_dates.append(target_date)

    if missing_from_api:
        raise RuntimeError(
            "Alternative.me did not provide complete coverage for missing dates: "
            + ", ".join(missing_from_api[:20])
        )

    catalog = os.getenv("DATABRICKS_CATALOG", "crypto-data-pipeline")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_FEAR_GREED_TABLE", "fear_greed_raw")
    schema_name = _qualified_schema_name(catalog, schema)
    table_name = _qualified_table_name(catalog, schema, table)

    with _connect_databricks() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    value INT,
                    value_classification STRING,
                    metric_date DATE,
                    time_until_update INT,
                    raw_json STRING,
                    ingestion_ts TIMESTAMP,
                    source STRING,
                    run_key STRING
                )
                USING DELTA
                """
            )
            _ensure_columns_exist(
                cursor,
                table_name,
                [
                    ("value", "INT"),
                    ("value_classification", "STRING"),
                    ("metric_date", "DATE"),
                    ("time_until_update", "INT"),
                    ("raw_json", "STRING"),
                    ("ingestion_ts", "TIMESTAMP"),
                    ("source", "STRING"),
                    ("run_key", "STRING"),
                ],
            )

            date_placeholders = ", ".join(["?"] * len(ingested_dates))
            cursor.execute(
                f"""
                DELETE FROM {table_name}
                WHERE source = ?
                  AND metric_date IN ({date_placeholders})
                """,
                ["alternative_me_api", *ingested_dates],
            )

            _insert_records_in_batches(
                cursor,
                table_name,
                records,
                [
                    "value",
                    "value_classification",
                    "metric_date",
                    "time_until_update",
                    "raw_json",
                    "ingestion_ts",
                    "source",
                    "run_key",
                ],
                batch_size=100,
            )

    return {
        "rows_written": len(records),
        "missing_dates_count": len(missing_dates),
        "ingested_dates": [d.isoformat() for d in ingested_dates],
    }


def _fetch_chart_points(chart_name, timespan_days):
    response = requests.get(
        f"https://api.blockchain.info/charts/{chart_name}",
        params={
            "timespan": f"{timespan_days}days",
            "format": "json",
            "sampled": "true",
        },
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Blockchain.com chart request failed for {chart_name} with status "
            f"{response.status_code}: {response.text[:500]}"
        )

    payload = response.json()
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, list):
        raise RuntimeError(f"Blockchain.com payload for {chart_name} is missing a values array")

    points = {}
    for item in values:
        if not isinstance(item, dict):
            continue
        timestamp = _safe_int(item.get("x"))
        point_value = _safe_float(item.get("y"))
        if timestamp is None or point_value is None:
            continue
        metric_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        points[metric_date] = point_value

    return points


def _ingest_missing_onchain_dates(missing_dates):
    if not missing_dates:
        return {
            "rows_written": 0,
            "missing_dates_count": 0,
            "ingested_dates": [],
        }

    min_missing_date = min(missing_dates)
    required_days = (datetime.now(timezone.utc).date() - min_missing_date).days + 7
    configured_days = _safe_int(os.getenv("BLOCKCHAIN_CHART_TIMESPAN_DAYS")) or 120
    timespan_days = max(required_days, configured_days)

    points_by_metric = {}
    for metric_name, chart_name in CHARTS.items():
        points_by_metric[metric_name] = _fetch_chart_points(chart_name, timespan_days)

    ingestion_ts = datetime.now(timezone.utc)
    records = []
    missing_from_api = []
    ingested_dates = []
    for target_date in missing_dates:
        row_values = {}
        missing_metrics = []
        for metric_name in CHARTS:
            points = points_by_metric.get(metric_name, {})
            if target_date not in points:
                missing_metrics.append(metric_name)
                continue
            row_values[metric_name] = points[target_date]

        if missing_metrics:
            missing_from_api.append(f"{target_date.isoformat()} ({', '.join(missing_metrics)})")
            continue

        run_key = _build_run_key("backfill_last_month_onchain", target_date)
        records.append(
            (
                target_date,
                row_values.get("btc_tx_count"),
                row_values.get("btc_unique_addresses"),
                row_values.get("btc_hash_rate"),
                row_values.get("btc_mempool_size"),
                json.dumps({"metric_date": target_date.isoformat(), "values": row_values}),
                ingestion_ts,
                "blockchain_info_charts_api",
                run_key,
            )
        )
        ingested_dates.append(target_date)

    if missing_from_api:
        raise RuntimeError(
            "Blockchain.com did not provide complete coverage for missing dates: "
            + ", ".join(missing_from_api[:20])
        )

    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_ONCHAIN_TABLE", "onchain_macro_raw")
    schema_name = _qualified_schema_name(catalog, schema)
    table_name = _qualified_table_name(catalog, schema, table)

    with _connect_databricks() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    metric_date DATE,
                    btc_tx_count DOUBLE,
                    btc_unique_addresses DOUBLE,
                    btc_hash_rate DOUBLE,
                    btc_mempool_size DOUBLE,
                    raw_json STRING,
                    ingestion_ts TIMESTAMP,
                    source STRING,
                    run_key STRING
                )
                USING DELTA
                """
            )
            _ensure_columns_exist(
                cursor,
                table_name,
                [
                    ("metric_date", "DATE"),
                    ("btc_tx_count", "DOUBLE"),
                    ("btc_unique_addresses", "DOUBLE"),
                    ("btc_hash_rate", "DOUBLE"),
                    ("btc_mempool_size", "DOUBLE"),
                    ("raw_json", "STRING"),
                    ("ingestion_ts", "TIMESTAMP"),
                    ("source", "STRING"),
                    ("run_key", "STRING"),
                ],
            )

            date_placeholders = ", ".join(["?"] * len(ingested_dates))
            cursor.execute(
                f"""
                DELETE FROM {table_name}
                WHERE source = ?
                  AND metric_date IN ({date_placeholders})
                """,
                ["blockchain_info_charts_api", *ingested_dates],
            )

            _insert_records_in_batches(
                cursor,
                table_name,
                records,
                [
                    "metric_date",
                    "btc_tx_count",
                    "btc_unique_addresses",
                    "btc_hash_rate",
                    "btc_mempool_size",
                    "raw_json",
                    "ingestion_ts",
                    "source",
                    "run_key",
                ],
                batch_size=200,
            )

    return {
        "rows_written": len(records),
        "missing_dates_count": len(missing_dates),
        "ingested_dates": [d.isoformat() for d in ingested_dates],
    }


def backfill_missing_last_month(**context):
    task_start = time.perf_counter()
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    backfill_start_date, backfill_end_date = _resolve_window_dates(conf)
    missing_by_source = _detect_missing_dates(backfill_start_date, backfill_end_date)

    coingecko_result = _ingest_missing_coingecko_dates(missing_by_source["coingecko"])
    fear_greed_result = _ingest_missing_fear_greed_dates(missing_by_source["fear_greed"])
    onchain_result = _ingest_missing_onchain_dates(missing_by_source["onchain"])

    total_rows_written = (
        coingecko_result["rows_written"]
        + fear_greed_result["rows_written"]
        + onchain_result["rows_written"]
    )

    lookback_days = (backfill_end_date - backfill_start_date).days + 1
    summary = {
        "window_start": backfill_start_date.isoformat(),
        "window_end": backfill_end_date.isoformat(),
        "coingecko": coingecko_result,
        "fear_greed": fear_greed_result,
        "onchain": onchain_result,
        "total_rows_written": total_rows_written,
        "silver_reprocess_days": lookback_days,
        "gold_reprocess_days": lookback_days,
        "runtime_seconds": round(time.perf_counter() - task_start, 2),
    }

    print("Last month missing-date backfill summary:")
    print(json.dumps(summary, indent=2))
    return summary


def drop_deprecated_gold_columns():
    gold_catalog = os.getenv("DATABRICKS_GOLD_CATALOG", "crypto-data-pipeline")
    gold_schema = os.getenv("DATABRICKS_GOLD_SCHEMA", "gold")

    cleanup_targets = [
        ("gold_liquidity_ranking", "high_cap_anomaly_flag"),
        ("gold_market_summary", "return_7d_pct"),
        ("gold_market_summary", "return_30d_pct"),
        ("gold_volatility_signal", "volatility_signal"),
        ("gold_volatility_signal", "volatility_7d"),
        ("gold_volatility_signal", "volatility_30d"),
    ]

    with _connect_databricks() as connection:
        with connection.cursor() as cursor:
            for table_name, column_name in cleanup_targets:
                qualified_table = _qualified_table_name(gold_catalog, gold_schema, table_name)
                quoted_column = _quote_identifier(column_name)
                cursor.execute(
                    f"""
                    ALTER TABLE {qualified_table}
                    DROP COLUMN IF EXISTS {quoted_column}
                    """
                )
                print(
                    f"Ensured deprecated column is removed: "
                    f"{gold_catalog}.{gold_schema}.{table_name}.{column_name}"
                )

    return {
        "gold_catalog": gold_catalog,
        "gold_schema": gold_schema,
        "dropped_columns": [f"{table}.{column}" for table, column in cleanup_targets],
    }


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21, tzinfo=pendulum.timezone("Europe/Madrid")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert,
}


with DAG(
    dag_id="backfill_last_month_missing_crypto_bronze_silver_gold",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    description=(
        "Manual one-time backfill that checks last month coverage in bronze, ingests missing "
        "dates per source, then runs silver and gold transformations on the recovered window."
    ),
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_missing_bronze_last_month = PythonOperator(
        task_id="ingest_missing_bronze_last_month",
        python_callable=backfill_missing_last_month,
        execution_timeout=timedelta(hours=8),
    )

    trigger_silver_transformations = TriggerDagRunOperator(
        task_id="trigger_silver_transformations",
        trigger_dag_id="transform_silver_crypto_models",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=120),
        conf={
            "silver_reprocess_days": "{{ ti.xcom_pull(task_ids='ingest_missing_bronze_last_month').get('silver_reprocess_days', 31) }}",
        },
    )

    trigger_gold_transformations = TriggerDagRunOperator(
        task_id="trigger_gold_transformations",
        trigger_dag_id="transform_gold_crypto_models",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=120),
        conf={
            "gold_reprocess_days": "{{ ti.xcom_pull(task_ids='ingest_missing_bronze_last_month').get('gold_reprocess_days', 31) }}",
        },
    )

    cleanup_deprecated_gold_columns = PythonOperator(
        task_id="cleanup_deprecated_gold_columns",
        python_callable=drop_deprecated_gold_columns,
        execution_timeout=timedelta(minutes=20),
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_missing_bronze_last_month >> trigger_silver_transformations
    trigger_silver_transformations >> trigger_gold_transformations
    trigger_gold_transformations >> cleanup_deprecated_gold_columns >> end
