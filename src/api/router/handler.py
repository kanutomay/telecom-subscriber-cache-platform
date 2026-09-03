# Name: handler.py
# Service: router
# Description: Single API Gateway entry point. Inspects INTENT_NAME (and
#   MARKET, where a service is deployed per-market) on the incoming request
#   and synchronously invokes the matching downstream Lambda, so the
#   platform exposes one public endpoint while each lookup service scales,
#   deploys, and fails independently.
#
# This is a sanitized, standalone reconstruction of a production router
# Lambda, scoped to the services documented in this repository. The
# original production router also dispatches to several unrelated internal
# systems (billing, ticketing, field-ops tooling) that are out of scope for
# this platform and have been omitted rather than sanitized in place.
"""
Runtime settings: Python 3.9+
Memory: 128 MB / Timeout: ~25s (tight, since every request pays one
        synchronous Lambda-to-Lambda hop)

Expected event structure:
    {
        "CORRELATION_ID": "b6f1...",
        "MARKET": "US",
        "INTENT_NAME": "HEADER-LOOKUP",   // routing key, see INTENT_ROUTES below
        "CHANNEL": "ivr",
        ...service-specific fields...
    }
"""
import json
import time
import uuid
import logging
import resource
import traceback

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client("lambda")

# Routing table: (INTENT_NAME, MARKET | None) -> downstream function name.
# A `None` market means "route regardless of market". More specific
# (intent, market) entries are checked before the market-agnostic fallback,
# which lets a market override the default implementation (e.g. a
# market-specific header-lookup service) without touching the router logic.
INTENT_ROUTES = {
    ("HEADER-LOOKUP", None): "header_lookup",
    ("RECORD-LOOKUP", None): "record_lookup",
    ("SMS-NOTIF", None): "sms_notification",
    ("PLACES-SEARCH", None): "places_search",
}


def lambda_handler(event, context):
    start_time = time.time()
    request_id = str(uuid.uuid4())
    event = {**event, "request_id": request_id}

    # Log routing metadata only — never the raw event or the downstream
    # result. Both can carry customer PII (account ID, phone, personal ID
    # in the request; full account records, including billing address and
    # ID values, in a header/record-lookup response), and CloudWatch Logs
    # is not an appropriate place for that. CORRELATION_ID is safe to log
    # since it's an opaque tracing token, not an identifier by itself.
    intent = event.get("INTENT_NAME")
    market = event.get("MARKET")
    channel = event.get("CHANNEL")
    corr_id = event.get("CORRELATION_ID")
    logger.info(f"{request_id} router entry: intent={intent} market={market} channel={channel} correlation_id={corr_id}")

    result = None

    try:
        target_function = INTENT_ROUTES.get((intent, market)) or INTENT_ROUTES.get((intent, None))

        if target_function is None:
            result = {"statusCode": 400, "message": f"Unrecognized INTENT_NAME/MARKET: {intent}/{market}"}
        else:
            response = lambda_client.invoke(
                FunctionName=target_function,
                InvocationType="RequestResponse",
                Payload=json.dumps(event),
            )
            result = json.loads(response["Payload"].read().decode("utf-8"))

        _log_completion(context, start_time, intent, result)

    except Exception:
        # Full traceback goes to the internal log only; the caller gets a
        # generic message with no exception detail (see also header_lookup's
        # _response, which follows the same rule for its client-facing errors).
        logger.error(f"{request_id} router exception: {traceback.format_exc()}")
        result = {"statusCode": 500, "message": "Internal server error"}

    status_code = result.get("statusCode", "NA") if isinstance(result, dict) else "NA"
    logger.info(f"{request_id} router exit: intent={intent} status_code={status_code}")
    return result


def _log_completion(context, start_time, intent, result):
    """Emit a single structured completion line — request ID, duration, memory
    footprint, intent, and status — analogous to the Lambda platform's own
    REPORT line but joinable against application-level correlation IDs."""
    duration_ms = (time.time() - start_time) * 1000
    memory_used_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    status_code = result.get("statusCode", "NA") if isinstance(result, dict) else "NA"
    logger.info(
        f"RequestId: {getattr(context, 'aws_request_id', 'n/a')}, "
        f"Duration: {duration_ms:.2f} ms, "
        f"MemoryLimit: {getattr(context, 'memory_limit_in_mb', 'n/a')} MB, "
        f"MaxMemoryUsed: {memory_used_mb:.2f} MB, "
        f"Intent: {intent}, StatusCode: {status_code}"
    )
