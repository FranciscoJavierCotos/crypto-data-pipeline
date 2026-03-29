from datetime import datetime, date, time as dt_time, timedelta, timezone
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


def _yesterday_in_madrid():
    return pendulum.now("Europe/Madrid").subtract(days=1).date()


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
    demo_api_key = (os.getenv("COINGECKO_API_KEY") or "").strip()
    if demo_api_key:
        return {"x-cg-demo-api-key": demo_api_key}
    return {}


def _http_get_json_with_retries(
    url,
    params,
    timeout=30,
    retries=4,
    backoff_seconds=2.0,
    headers=None,
    swallow_http_statuses=None,
):
    last_error_text = None
    swallow_http_statuses = set(swallow_http_statuses or [])
    for attempt in range(1, retries + 1):
        response = requests.get(url, params=params, timeout=timeout, headers=headers)

        if response.status_code in swallow_http_statuses:
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


def ingest_yesterday_coingecko_to_bronze():
    total_start = time.perf_counter()
    target_date = _yesterday_in_madrid()
    target_date_text = target_date.strftime("%d-%m-%Y")
    ingestion_ts = datetime.now(timezone.utc)
    run_key = _build_run_key("backfill_yesterday_coingecko", target_date)
    source = "coingecko_api"
    headers = _coingecko_headers()
    has_api_key = bool(headers)
    default_request_delay_seconds = 1.5 if has_api_key else 6.0
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
    )
    if not isinstance(top_coins, list) or not top_coins:
        raise RuntimeError("CoinGecko top coins response was empty")
    top_coins = top_coins[:top_n_coins]

    history_url_template = "https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    history_params = {
        "date": target_date_text,
        "localization": "false",
    }

    records_by_id = {}
    skipped_coin_ids = []
    failed_coin_ids = []
    for coin in top_coins:
        coin_id = coin.get("id") if isinstance(coin, dict) else None
        if not coin_id:
            continue

        history_url = history_url_template.format(coin_id=coin_id)
        try:
            payload = _http_get_json_with_retries(
                history_url,
                params=history_params,
                timeout=30,
                retries=history_retries,
                backoff_seconds=2.0,
                headers=headers,
                swallow_http_statuses={404},
            )
        except RuntimeError as exc:
            print(f"Skipping coin {coin_id} after retry exhaustion: {exc}")
            failed_coin_ids.append(coin_id)
            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)
            continue

        if payload is None:
            skipped_coin_ids.append(coin_id)
            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)
            continue

        market_data = payload.get("market_data") if isinstance(payload, dict) else None
        current_price = _safe_float(_extract_usd_value(market_data, "current_price"))
        if current_price is None:
            skipped_coin_ids.append(coin_id)
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
            source,
            run_key,
        )

        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)

    records = list(records_by_id.values())
    if not records:
        raise RuntimeError("CoinGecko yesterday backfill produced zero valid rows")
    if len(records) < min_successful_rows:
        raise RuntimeError(
            f"CoinGecko yesterday backfill produced only {len(records)} rows; "
            f"minimum required is {min_successful_rows}. Failed coins: {failed_coin_ids[:10]}"
        )

    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_TABLE", "coingecko_market_data")
    schema_name = _qualified_schema_name(catalog, schema)
    table_name = _qualified_table_name(catalog, schema, table)

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

            cursor.execute(
                f"""
                DELETE FROM {table_name}
                WHERE source = ?
                  AND CAST(last_updated AS DATE) = ?
                """,
                [source, target_date],
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
            _insert_records_in_batches(
                cursor,
                table_name,
                records,
                column_names,
                batch_size=100,
            )

            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE run_key = ?",
                [run_key],
            )
            inserted_count_row = cursor.fetchone()
            inserted_count = inserted_count_row[0] if inserted_count_row else 0
            if inserted_count != len(records):
                raise RuntimeError(
                    f"CoinGecko backfill row count mismatch for run_key {run_key}: "
                    f"expected {len(records)}, found {inserted_count}"
                )

    print(
        f"CoinGecko yesterday backfill completed for {target_date.isoformat()} with "
        f"{len(records)} rows; skipped {len(skipped_coin_ids)} coins; failed {len(failed_coin_ids)} coins"
    )
    print(
        f"CoinGecko backfill settings: top_n_coins={top_n_coins}, "
        f"request_delay_seconds={request_delay_seconds}, retries={history_retries}, api_key={'yes' if has_api_key else 'no'}"
    )
    print(f"Total runtime: {time.perf_counter() - total_start:.2f}s")
    print(f"Ingestion run_key: {run_key}")
    return run_key


def ingest_yesterday_fear_greed_to_bronze():
    total_start = time.perf_counter()
    target_date = _yesterday_in_madrid()
    ingestion_ts = datetime.now(timezone.utc)
    run_key = _build_run_key("backfill_yesterday_fear_greed", target_date)
    source = "alternative_me_api"

    response = requests.get("https://api.alternative.me/fng/?limit=30&format=json", timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Alternative.me request failed with status {response.status_code}: {response.text[:500]}"
        )

    payload = response.json()
    data_points = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_points, list) or not data_points:
        raise RuntimeError("Alternative.me payload is missing a non-empty data array")

    target_point = None
    for point in data_points:
        if not isinstance(point, dict):
            continue
        metric_timestamp = _safe_int(point.get("timestamp"))
        if metric_timestamp is None:
            continue
        metric_day = datetime.fromtimestamp(metric_timestamp, tz=timezone.utc).date()
        if metric_day == target_date:
            target_point = point
            break

    if target_point is None:
        raise RuntimeError(
            f"Alternative.me did not return a record for yesterday ({target_date.isoformat()})"
        )

    value = _safe_int(target_point.get("value"))
    value_classification = target_point.get("value_classification")
    if value is None or not value_classification:
        raise RuntimeError("Alternative.me yesterday record is missing required fields")

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

            cursor.execute(
                f"""
                MERGE INTO {table_name} AS target
                USING (
                    SELECT
                        ? AS value,
                        ? AS value_classification,
                        ? AS metric_date,
                        ? AS time_until_update,
                        ? AS raw_json,
                        ? AS ingestion_ts,
                        ? AS source,
                        ? AS run_key
                ) AS source
                ON target.metric_date = source.metric_date AND target.source = source.source
                WHEN MATCHED AND source.ingestion_ts >= target.ingestion_ts THEN
                  UPDATE SET
                    target.value = source.value,
                    target.value_classification = source.value_classification,
                    target.time_until_update = source.time_until_update,
                    target.raw_json = source.raw_json,
                    target.ingestion_ts = source.ingestion_ts,
                    target.source = source.source,
                    target.run_key = source.run_key
                WHEN NOT MATCHED THEN
                  INSERT (
                    value,
                    value_classification,
                    metric_date,
                    time_until_update,
                    raw_json,
                    ingestion_ts,
                    source,
                    run_key
                  )
                  VALUES (
                    source.value,
                    source.value_classification,
                    source.metric_date,
                    source.time_until_update,
                    source.raw_json,
                    source.ingestion_ts,
                    source.source,
                    source.run_key
                  )
                """,
                [
                    value,
                    value_classification,
                    target_date,
                    _safe_int(target_point.get("time_until_update")),
                    json.dumps(target_point),
                    ingestion_ts,
                    source,
                    run_key,
                ],
            )

    print(
        f"Fear & Greed yesterday backfill completed for {target_date.isoformat()} with value {value}"
    )
    print(f"Total runtime: {time.perf_counter() - total_start:.2f}s")
    print(f"Ingestion run_key: {run_key}")
    return run_key


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


def ingest_yesterday_onchain_to_bronze():
    total_start = time.perf_counter()
    target_date = _yesterday_in_madrid()
    ingestion_ts = datetime.now(timezone.utc)
    run_key = _build_run_key("backfill_yesterday_onchain", target_date)
    source = "blockchain_info_charts_api"
    timespan_days = _safe_int(os.getenv("BLOCKCHAIN_CHART_TIMESPAN_DAYS")) or 120

    row_values = {}
    for metric_name, chart_name in CHARTS.items():
        points = _fetch_chart_points(chart_name, timespan_days)
        if target_date not in points:
            raise RuntimeError(
                f"Blockchain.com chart {chart_name} does not contain yesterday "
                f"({target_date.isoformat()}) in the returned time window"
            )
        row_values[metric_name] = points[target_date]

    record = (
        target_date,
        row_values.get("btc_tx_count"),
        row_values.get("btc_unique_addresses"),
        row_values.get("btc_hash_rate"),
        row_values.get("btc_mempool_size"),
        json.dumps({"metric_date": target_date.isoformat(), "values": row_values}),
        ingestion_ts,
        source,
        run_key,
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

            cursor.execute(
                f"""
                DELETE FROM {table_name}
                WHERE source = ? AND metric_date = ?
                """,
                [source, target_date],
            )

            _insert_records_in_batches(
                cursor,
                table_name,
                [record],
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
                batch_size=1,
            )

    print(f"On-chain yesterday backfill completed for {target_date.isoformat()}")
    print(f"Total runtime: {time.perf_counter() - total_start:.2f}s")
    print(f"Ingestion run_key: {run_key}")
    return run_key


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
    dag_id="backfill_yesterday_crypto_bronze_silver_gold",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    description="Manual full pipeline backfill that ingests yesterday-only source data and runs silver/gold.",
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_coingecko_yesterday = PythonOperator(
        task_id="ingest_coingecko_yesterday",
        python_callable=ingest_yesterday_coingecko_to_bronze,
        execution_timeout=timedelta(minutes=90),
    )

    trigger_quality_after_coingecko = TriggerDagRunOperator(
        task_id="trigger_quality_after_coingecko",
        trigger_dag_id="quality_checks_crypto_data_layers",
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "coingecko",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_coingecko_yesterday') }}",
            "fail_on_non_critical": False,
        },
    )

    ingest_fear_greed_yesterday = PythonOperator(
        task_id="ingest_fear_greed_yesterday",
        python_callable=ingest_yesterday_fear_greed_to_bronze,
        execution_timeout=timedelta(minutes=15),
    )

    trigger_quality_after_fear_greed = TriggerDagRunOperator(
        task_id="trigger_quality_after_fear_greed",
        trigger_dag_id="quality_checks_crypto_data_layers",
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "fear_greed",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_fear_greed_yesterday') }}",
            "fail_on_non_critical": False,
        },
    )

    ingest_onchain_yesterday = PythonOperator(
        task_id="ingest_onchain_yesterday",
        python_callable=ingest_yesterday_onchain_to_bronze,
        execution_timeout=timedelta(minutes=20),
    )

    trigger_quality_after_onchain = TriggerDagRunOperator(
        task_id="trigger_quality_after_onchain",
        trigger_dag_id="quality_checks_crypto_data_layers",
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "onchain",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_onchain_yesterday') }}",
            "fail_on_non_critical": False,
        },
    )

    trigger_silver_transformations = TriggerDagRunOperator(
        task_id="trigger_silver_transformations",
        trigger_dag_id="transform_silver_crypto_models",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    trigger_gold_transformations = TriggerDagRunOperator(
        task_id="trigger_gold_transformations",
        trigger_dag_id="transform_gold_crypto_models",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    end = EmptyOperator(task_id="end")

    start >> [
        ingest_coingecko_yesterday,
        ingest_fear_greed_yesterday,
        ingest_onchain_yesterday,
    ]

    ingest_coingecko_yesterday >> trigger_quality_after_coingecko
    ingest_fear_greed_yesterday >> trigger_quality_after_fear_greed
    ingest_onchain_yesterday >> trigger_quality_after_onchain

    [
        ingest_coingecko_yesterday,
        ingest_fear_greed_yesterday,
        ingest_onchain_yesterday,
    ] >> trigger_silver_transformations

    trigger_silver_transformations >> trigger_gold_transformations >> end
