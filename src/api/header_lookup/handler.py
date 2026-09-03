# Name: handler.py
# Service: header_lookup
# Description: Lambda function that resolves every account/phone/personal-ID
#   association for a caller, so a client can disambiguate a multi-account
#   customer before requesting full account detail. Backed entirely by
#   Redis/Valkey primary + secondary index reads (no data-lake or DB calls
#   on the hot path).
#
# This is a sanitized, standalone reconstruction of a production Lambda.
# Real AWS account IDs, VPC/subnet/security-group IDs, and cluster endpoints
# have been replaced with placeholders — see README "Author's Note".
#
"""
Runtime settings: Python 3.9+
Layers: package the 'redis' library (redis-py) into a Lambda layer or the
        deployment bundle.
VPC: attach to a subnet/security group with network access to your
     ElastiCache (Valkey/Redis) cluster.

Expected event structure (at least one of ACCOUNT_ID / PHONE_NUMBER /
PERSONAL_ID must be provided, checked in that priority order):
    {
        "CORRELATION_ID": "b6f1...",     // optional, auto-generated if absent
        "MARKET": "US",                  // for logging/routing only
        "INTENT_NAME": "HEADER-LOOKUP",
        "CHANNEL": "ivr",                // client identifier, for logging
        "ACCOUNT_ID": "ACCT-DEMO-0001",  // optional
        "PHONE_NUMBER": "555-0100",      // optional
        "PERSONAL_ID": "ABC-123-XYZ"     // optional
    }
"""
import os
import sys
import json
import re
import logging
from datetime import datetime, timedelta, timezone
import uuid

import redis

from structured_logger import structured_log

# --- Logging setup ---------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

ENV = os.getenv("ENVIRONMENT", "production")
if ENV not in ("production", "staging", "development"):
    logger.error(f"Invalid environment: {ENV}. Must be one of 'production', 'staging', 'development'.")
    sys.exit(1)

# Response timestamps are rendered in this operating timezone; make it a
# config value rather than a hardcoded offset per deployment.
RESPONSE_TZ = timezone(timedelta(hours=int(os.getenv("RESPONSE_TZ_OFFSET_HOURS", "0"))))

# National-format phone numbers may arrive with the country calling code
# prepended (e.g. from a chat/voice bot export outside the primary market).
# Configure this per deployment rather than hardcoding a specific market's
# dialing code.
COUNTRY_CALLING_CODE = os.getenv("COUNTRY_CALLING_CODE", "")  # e.g. "507"
NATIONAL_NUMBER_LENGTHS = {7, 8}  # digits, after country-code stripping

# --- Redis connection (initialized once per warm Lambda container) --------
# Authentication and in-transit TLS are intentionally omitted from this
# reconstruction — configure them for your deployment rather than assuming
# network isolation (private subnet + security group) is enough on its own.
# It rules out access from outside the VPC but not from a compromised
# neighbor inside it; pair it with Redis AUTH/IAM auth and ssl=True unless
# you have a specific reason not to (see also src/etl/subscriber_identity_index/load_job.py).
REDIS_ENDPOINT = os.getenv("REDIS_ENDPOINT", "your-cluster.xxxxxxx.use1.cache.amazonaws.com")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_SOCKET_TIMEOUT = 5
REDIS_SOCKET_KEEPALIVE = True

try:
    redis_client = redis.StrictRedis(
        host=REDIS_ENDPOINT,
        port=REDIS_PORT,
        db=REDIS_DB,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
        socket_keepalive=REDIS_SOCKET_KEEPALIVE,
        decode_responses=True,
    )
    redis_client.ping()
    # Deliberately not logging REDIS_ENDPOINT: it's not PII, but a resolved
    # internal hostname is topology detail that has no reason to sit in
    # application logs even when it isn't sensitive on its own.
    logger.info(f"Connected to Redis (DB {REDIS_DB})")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_client = None


def lambda_handler(event, context):
    start_time = datetime.now(timezone.utc)
    aws_request_id = getattr(context, "aws_request_id", "n/a") if context else "n/a"

    # Unwrap API Gateway proxy body if present
    if isinstance(event.get("body"), str):
        try:
            payload = json.loads(event["body"])
        except json.JSONDecodeError:
            _log("ERROR", aws_request_id, None, "Unknown", "HEADER-LOOKUP", "api_error",
                 "Invalid JSON in request body.", start_time, 400)
            return _response(400, {"error": "Invalid JSON in request body"})
    else:
        payload = event

    payload = {k.strip().lower(): v for k, v in payload.items()}

    corr_id = payload.get("correlation_id")
    corr_id = corr_id.strip() if isinstance(corr_id, str) else None
    if not corr_id:
        corr_id = f"auto-gen-{uuid.uuid4().hex}"

    market = payload.get("market", "US").upper()
    intent = payload.get("intent_name", "HEADER-LOOKUP").upper()
    client_channel = payload.get("channel", "Unknown")
    acct_id = payload.get("account_id")
    phone = payload.get("phone_number")
    pid = payload.get("personal_id")

    _log("INFO", aws_request_id, corr_id, client_channel, intent, "api_request",
         "Received header lookup request", start_time, None, market=market)

    if redis_client is None:
        _log("ERROR", aws_request_id, corr_id, client_channel, intent, "api_error",
             "Redis client not initialized", start_time, 500, market=market)
        return _response(500, {"error": "Redis client not initialized"})

    acct_id = _clean(acct_id, r"[^A-Za-z0-9]")
    phone = _clean(phone, r"[^0-9]")
    pid = _clean(pid, r"[^A-Za-z0-9-]")
    phone = _normalize_phone(phone)

    if not any([acct_id, phone, pid]):
        _log("ERROR", aws_request_id, corr_id, client_channel, intent, "api_error",
             "At least one of account_id, phone_number, or personal_id must be provided.",
             start_time, 400, market=market)
        return _response(400, {"error": "At least one of account_id, phone_number, or personal_id must be provided."}, corr_id)

    try:
        matched_by, accounts = None, []

        # Priority: direct account ID, then phone (secondary index), then
        # personal ID (secondary index). First match wins.
        if acct_id:
            accounts = _fetch_accounts_by_ids([acct_id])
            if accounts:
                matched_by = "account_id"

        if not accounts and phone:
            accounts = _fetch_accounts_by_ids(_fetch_secondary_ids("phone", phone))
            if accounts:
                matched_by = "phone_number"

        if not accounts and pid:
            accounts = _fetch_accounts_by_ids(_fetch_secondary_ids("pid", pid))
            if accounts:
                matched_by = "personal_id"

        formatted_accounts = [_format_account(a) for a in accounts]

        if not formatted_accounts:
            _log("INFO", aws_request_id, corr_id, client_channel, intent, "api_response",
                 "No accounts found", start_time, 404, market=market)
            return _response(404, {
                "error": "No accounts found",
                "metadata": _metadata(corr_id, matched_by),
            }, corr_id)

        response_body = {
            "total_unique_accounts": len(formatted_accounts),
            "subscription_profile_type": _classify_profile(formatted_accounts),
            "accounts": formatted_accounts,
            "metadata": _metadata(corr_id, matched_by),
        }

        _log("INFO", aws_request_id, corr_id, client_channel, intent, "api_response",
             "Successfully retrieved accounts", start_time, 200, market=market)
        return _response(200, response_body, corr_id)

    except Exception:
        # Full exception + traceback goes to the internal log only.
        # The client gets a generic message — an exception string can leak
        # internals (a Redis host, a stray field value) that have no business
        # in an API response.
        logger.exception("Unexpected error during Redis query")
        _log("ERROR", aws_request_id, corr_id, client_channel, intent, "api_error",
             "Unexpected error during Redis query", start_time, 500, market=market)
        return _response(500, {"error": "Internal server error"}, corr_id)


# --- Helpers -----------------------------------------------------------


def _clean(value, disallowed_pattern):
    if not value:
        return None
    return re.sub(disallowed_pattern, "", value.strip())


def _normalize_phone(phone):
    """Strip a leading country calling code from a national-format number,
    when one is configured and the resulting length looks like a national
    subscriber number for this market."""
    if not phone or not COUNTRY_CALLING_CODE:
        return phone
    if phone.startswith(COUNTRY_CALLING_CODE):
        candidate = phone[len(COUNTRY_CALLING_CODE):]
        if len(candidate) in NATIONAL_NUMBER_LENGTHS:
            return candidate
    return phone


def _fetch_secondary_ids(key_prefix, identifier):
    """Secondary indexes are Redis Sets: <prefix>:<identifier> -> {account_id, ...}."""
    redis_set_key = f"{key_prefix}:{identifier}"
    members = redis_client.smembers(redis_set_key)
    return list(members) if members else []


def _fetch_accounts_by_ids(acct_ids):
    """Primary index is a Redis String per account: acct_id:<id> -> JSON blob."""
    accounts = []
    for acct_id in acct_ids:
        raw = redis_client.get(f"acct_id:{acct_id}")
        if raw:
            try:
                accounts.append(json.loads(raw))
            except (TypeError, ValueError):
                accounts.append(raw)
    return accounts


def _format_account(data):
    if not isinstance(data, dict):
        return {"raw": data}

    def split_phones(val):
        return [p.strip() for p in val.split(",") if p.strip()] if val else []

    fixed_phones = split_phones(data.get("fixed_svc_phone_list"))
    mobile_phones = split_phones(data.get("mobile_svc_phone_list"))
    contact_phones = split_phones(data.get("contact_phone_list"))

    return {
        "account_id": data.get("account_id"),
        "account_name": data.get("account_name", ""),
        "subscription_type": data.get("subscription_type", "UNKNOWN"),
        "customer_segment": data.get("customer_segment"),
        "contact_info": {
            "fixed_svc_phones": fixed_phones,
            "mobile_svc_phones": mobile_phones,
            "all_svc_phones": list(set(fixed_phones + mobile_phones)),
            "contact_phones": contact_phones,
        },
        "identification": {
            "id_type": data.get("id_type"),
            "id_value": data.get("id_value"),
        },
        "services": {
            "fixed_broadband": data.get("fixed_broadband_flag") == "1",
            "fixed_telephony": data.get("fixed_telephony_flag") == "1",
            "fixed_tv": data.get("fixed_tv_flag") == "1",
            "mobile_telephony": data.get("mobile_telephony_flag") == "1",
        },
    }


def _classify_profile(formatted_accounts):
    postpaid = [a for a in formatted_accounts if a.get("subscription_type") == "POSTPAID"]
    prepaid = [a for a in formatted_accounts if a.get("subscription_type") == "PREPAID"]

    if postpaid:
        has_fixed = any(
            a["services"]["fixed_broadband"] or a["services"]["fixed_telephony"] or a["services"]["fixed_tv"]
            for a in postpaid
        )
        has_mobile = any(a["services"]["mobile_telephony"] for a in postpaid)
        if has_fixed and has_mobile:
            return "FMC"
        if has_fixed:
            return "FIXED-ONLY"
        if has_mobile:
            return "MOBILE-ONLY"
        return "NONE"
    if prepaid and len(prepaid) == len(formatted_accounts):
        return "PREPAID"
    return "NONE"


def _metadata(corr_id, matched_by):
    return {
        "correlation_id": corr_id,
        "timestamp": datetime.now(RESPONSE_TZ).isoformat(),
        "matched_by": matched_by,
    }


def _response(status_code: int, body: dict, corr_id: str | None = None):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        # Lambda proxy integration requires body to already be a JSON string,
        # not an object — API Gateway does not serialize it for you.
        "body": json.dumps(body),
    }


def _log(level, aws_request_id, corr_id, channel, intent, event_type, message, start_time, status_code, market="US"):
    structured_log(
        project="subscriber-cache-platform",
        opco=market,
        env=ENV,
        resource_type="lambda",
        resource_name="header_lookup",
        aws_request_id=aws_request_id,
        log_level=level,
        service_name="header-lookup",
        correlation_id=corr_id,
        client_channel=channel,
        intent_name=intent,
        event_type=event_type,
        event_message=message,
        start_time=start_time,
        status_code=status_code,
    )
