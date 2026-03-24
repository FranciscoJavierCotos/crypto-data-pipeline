import json
import logging
import os
from datetime import datetime
from urllib import request


def _safe_iso(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _post_webhook(event_type, payload):
    webhook_url = (os.getenv("PIPELINE_ALERT_WEBHOOK_URL") or "").strip()
    if not webhook_url:
        return

    body = json.dumps({"event_type": event_type, "payload": payload}).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=10):
            pass
    except Exception:
        logging.exception("Failed posting pipeline alert webhook for event=%s", event_type)


def task_failure_alert(context):
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

    logging.error("Task failure alert: %s", json.dumps(payload, ensure_ascii=True))
    _post_webhook("task_failure", payload)


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    payload = {
        "dag_id": getattr(dag, "dag_id", None),
        "task_list": list(task_list or []),
        "blocking_task_list": list(blocking_task_list or []),
        "sla_count": len(list(slas or [])),
        "blocking_ti_count": len(list(blocking_tis or [])),
    }

    logging.error("SLA miss alert: %s", json.dumps(payload, ensure_ascii=True))
    _post_webhook("sla_miss", payload)
