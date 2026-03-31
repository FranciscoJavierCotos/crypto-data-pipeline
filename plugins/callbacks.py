# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Moved from dags/pipeline_callbacks.py to plugins/callbacks.py.
#   This file is NOT a DAG — it provides callback functions for
#   on_failure_callback and sla_miss_callback across all DAGs.
#
# WHY:
#   - plugins/ is on Airflow's PYTHONPATH automatically.
#   - Prevents the Airflow scheduler from trying to parse it as a DAG file.
#   - Import as: from callbacks import task_failure_alert, sla_miss_alert
# ---------------------------------------------------------------------------
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from urllib import request as urllib_request

logger = logging.getLogger(__name__)


def _safe_iso(value) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _post_webhook(event_type: str, payload: dict) -> None:
    webhook_url = (os.getenv("PIPELINE_ALERT_WEBHOOK_URL") or "").strip()
    if not webhook_url:
        return

    body = json.dumps({"event_type": event_type, "payload": payload}).encode("utf-8")
    req = urllib_request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib_request.urlopen(req, timeout=10):
            pass
    except Exception:
        logger.exception("Failed posting pipeline alert webhook for event=%s", event_type)


def task_failure_alert(context: dict) -> None:
    """Airflow on_failure_callback — logs failure details and posts to webhook."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    payload = {
        "dag_id": getattr(task_instance, "dag_id", None),
        "task_id": getattr(task_instance, "task_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "execution_date": _safe_iso(context.get("execution_date")),
        "exception": str(exception) if exception else None,
        "try_number": getattr(task_instance, "try_number", None),
        "log_url": getattr(task_instance, "log_url", None),
    }

    logger.error("Task failure alert: %s", json.dumps(payload, ensure_ascii=True))
    _post_webhook("task_failure", payload)


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Airflow sla_miss_callback — logs SLA breach and posts to webhook."""
    payload = {
        "dag_id": getattr(dag, "dag_id", None),
        "task_list": list(task_list or []),
        "blocking_task_list": list(blocking_task_list or []),
        "sla_count": len(list(slas or [])),
        "blocking_ti_count": len(list(blocking_tis or [])),
    }

    logger.error("SLA miss alert: %s", json.dumps(payload, ensure_ascii=True))
    _post_webhook("sla_miss", payload)


def dag_success_alert(context: dict) -> None:
    """Optional on_success_callback — posts a success event to webhook."""
    dag_run = context.get("dag_run")
    payload = {
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "execution_date": _safe_iso(context.get("execution_date")),
        "state": "success",
    }
    logger.info("DAG success: %s", json.dumps(payload, ensure_ascii=True))
    _post_webhook("dag_success", payload)
