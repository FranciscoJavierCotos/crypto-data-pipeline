# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   New DAG — provides a dedicated alerting entry point for pipeline events.
#
# WHY:
#   - Separates alerting configuration from individual DAG callbacks.
#   - Can be extended with Slack, PagerDuty, email integrations.
#   - Triggered by other DAGs on critical failures.
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from callbacks import task_failure_alert

logger = logging.getLogger(__name__)

DOC_MD = """
### Alerting DAG

Triggered by downstream DAGs or operators when critical alerts need to be sent.
Accepts `dag_run.conf` with alert details and routes them to configured channels.

**Parameters (via `dag_run.conf`):**
- `alert_type`: "failure" | "sla_breach" | "data_quality" | "custom"
- `message`: Human-readable alert message
- `severity`: "critical" | "warning" | "info"
- `source_dag_id`: Which DAG triggered the alert
"""

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="alerting",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["maintenance", "alerting"],
    doc_md=DOC_MD,
    params={
        "alert_type": "custom",
        "message": "",
        "severity": "warning",
        "source_dag_id": "",
    },
)
def alerting():

    @task(execution_timeout=timedelta(minutes=5))
    def process_alert(**context):
        import json
        import os
        from urllib import request as urllib_request

        conf = context.get("dag_run").conf or {}
        alert_type = conf.get("alert_type", "custom")
        message = conf.get("message", "No message provided")
        severity = conf.get("severity", "warning")
        source_dag = conf.get("source_dag_id", "unknown")

        logger.warning(
            "ALERT [%s] severity=%s source=%s: %s",
            alert_type, severity, source_dag, message,
        )

        webhook_url = (os.getenv("PIPELINE_ALERT_WEBHOOK_URL") or "").strip()
        if webhook_url:
            payload = json.dumps({
                "event_type": f"alert_{alert_type}",
                "payload": {
                    "alert_type": alert_type,
                    "message": message,
                    "severity": severity,
                    "source_dag_id": source_dag,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            }).encode("utf-8")
            req = urllib_request.Request(
                webhook_url, data=payload,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            try:
                with urllib_request.urlopen(req, timeout=10):
                    pass
                logger.info("Alert posted to webhook")
            except Exception:
                logger.exception("Failed to post alert webhook")

        return {"alert_type": alert_type, "severity": severity, "delivered": True}

    process_alert()


alerting()
