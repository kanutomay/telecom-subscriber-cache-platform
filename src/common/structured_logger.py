"""
Structured JSON logger used across the API and ETL services.

Every log line is a single JSON object carrying a consistent set of fields
(project, environment, resource, correlation ID, event type/message, status
code, duration) so logs can be filtered and joined across services in
CloudWatch / any log aggregator without per-service parsing rules.

This is a sanitized, standalone reconstruction of the shared logging module
referenced by src/api/header_lookup and the ETL jobs.
"""
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger()


def structured_log(
    *,
    project: str,
    opco: str,
    env: str,
    resource_type: str,
    resource_name: str,
    aws_request_id: str,
    log_level: str,
    service_name: str,
    correlation_id: str | None,
    client_channel: str,
    intent_name: str,
    event_type: str,
    event_message: str,
    start_time: datetime,
    status_code: int | None,
) -> None:
    """Emit one structured JSON log line for the current request."""
    now = datetime.now(timezone.utc)
    record = {
        "timestamp": now.isoformat(),
        "project": project,
        "opco": opco,
        "environment": env,
        "resource_type": resource_type,
        "resource_name": resource_name,
        "aws_request_id": aws_request_id,
        "service_name": service_name,
        "correlation_id": correlation_id,
        "client_channel": client_channel,
        "intent_name": intent_name,
        "event_type": event_type,
        "message": event_message,
        "status_code": status_code,
        "duration_ms": round((now - start_time).total_seconds() * 1000, 2) if start_time else None,
    }

    log_fn = {
        "DEBUG": logger.debug,
        "INFO": logger.info,
        "WARNING": logger.warning,
        "ERROR": logger.error,
        "CRITICAL": logger.critical,
    }.get(log_level.upper(), logger.info)

    log_fn(json.dumps(record, default=str))
