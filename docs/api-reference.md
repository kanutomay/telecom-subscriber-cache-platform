# API Reference

All requests go through a single API Gateway entry point (`src/api/router`),
which reads `INTENT_NAME` from the payload and dispatches synchronously to
the matching downstream Lambda. Every example below uses entirely synthetic
identifiers — no field, ID, name, or address in this document corresponds to
a real customer or account.

## Common request envelope

Every downstream service accepts the same base envelope; service-specific
fields are added on top of it.

| Field             | Type   | Required | Description                                                            |
|-------------------|--------|----------|--------------------------------------------------------------------------|
| `correlation_id`  | string | No       | Caller-supplied trace ID; auto-generated (`auto-gen-<uuid>`) if omitted. |
| `market`          | string | No       | Market/opco code, for logging and routing. Defaults to `US`.            |
| `intent_name`     | string | Yes      | Selects the downstream service (e.g. `HEADER-LOOKUP`).                  |
| `channel`         | string | No       | Calling client identifier (`ivr`, `chat`, `agent-tool`, …), for logging.|

Field names arrive case-insensitively and are lower-cased on receipt. All
services return a consistent Lambda proxy envelope on success:

```json
{
  "statusCode": 200,
  "headers": { "Content-Type": "application/json" },
  "body": "{\"...\": \"service-specific fields, plus a metadata block, JSON-encoded as a string\"}"
}
```

`body` is a JSON-encoded **string**, not a nested object — that's what API
Gateway's Lambda proxy integration requires, and it's what the client needs
to `JSON.parse`/`json.loads` before reading the fields below. The request/
response examples further down in this document show `body` already
decoded, for readability, rather than escaped.

A non-2xx `statusCode` comes with a `body` decoding to `{"error": "..."}`. Error
messages returned to the caller are always generic — full exception detail
is written to the service's own logs, never echoed back in the response
(see `src/api/header_lookup/handler.py` for the pattern).

---

## Header Lookup

Resolves every account associated with a caller-supplied identifier, so a
client can disambiguate a multi-account customer before requesting full
account detail. Reads only from the Redis/Valkey primary and secondary
indexes — no data-lake or database calls on the hot path. Implemented in
`src/api/header_lookup/handler.py`.

**Endpoint:** `POST /header-lookup` (via the router, `INTENT_NAME: HEADER-LOOKUP`)

### Request fields

| Field          | Type   | Required | Description                                                                 |
|----------------|--------|----------|-------------------------------------------------------------------------------|
| `account_id`   | string | One of these three | Direct account ID lookup (primary index).                       |
| `phone_number` | string | ...      | Service or contact phone number (secondary index, may match multiple accounts). |
| `personal_id`  | string | ...      | Government/personal ID on file (secondary index, may match multiple accounts).  |

At least one of `account_id`, `phone_number`, or `personal_id` must be
present; they're checked in that priority order and the first one that
resolves wins. A `phone_number` that starts with the configured country
calling code and leaves a national-length remainder has the code stripped
before lookup.

### Response fields

| Field                        | Type    | Description                                                              |
|-------------------------------|---------|----------------------------------------------------------------------------|
| `total_unique_accounts`      | integer | Count of accounts in `accounts`.                                          |
| `subscription_profile_type`  | string  | `FMC`, `FIXED-ONLY`, `MOBILE-ONLY`, `PREPAID`, or `NONE` — derived from the returned accounts' service flags. |
| `accounts[]`                 | array   | One entry per matched account (see below).                                |
| `metadata.correlation_id`    | string  | Echoes the request's correlation ID.                                      |
| `metadata.timestamp`         | string  | ISO-8601 response timestamp.                                              |
| `metadata.matched_by`        | string  | Which identifier produced the match: `account_id`, `phone_number`, or `personal_id`. `null` when nothing matched. |

Each entry in `accounts[]`:

```json
{
  "account_id": "ACCT-0000001",
  "account_name": "Example Customer",
  "subscription_type": "POSTPAID",
  "customer_segment": "RESIDENTIAL",
  "contact_info": {
    "fixed_svc_phones": ["555-0101"],
    "mobile_svc_phones": ["555-0110"],
    "all_svc_phones": ["555-0101", "555-0110"],
    "contact_phones": ["555-0199"]
  },
  "identification": {
    "id_type": "NATIONAL-ID",
    "id_value": "X-00-0000"
  },
  "services": {
    "fixed_broadband": true,
    "fixed_telephony": true,
    "fixed_tv": false,
    "mobile_telephony": true
  }
}
```

### Error codes

| Status | Meaning                                                                 |
|--------|--------------------------------------------------------------------------|
| 400    | Invalid JSON body, or none of `account_id` / `phone_number` / `personal_id` provided. |
| 404    | None of the provided identifiers matched an account. Body still includes `metadata` with `matched_by: null`. |
| 500    | Redis client unavailable, or an unexpected error during lookup. Response body is a generic `"Internal server error"` message; the real exception is logged server-side only. |

### Example

Request:

```json
{
  "intent_name": "HEADER-LOOKUP",
  "channel": "ivr",
  "market": "US",
  "phone_number": "5550101"
}
```

Response (`200`):

```json
{
  "total_unique_accounts": 1,
  "subscription_profile_type": "FIXED-ONLY",
  "accounts": [
    {
      "account_id": "ACCT-0000001",
      "account_name": "Example Customer",
      "subscription_type": "POSTPAID",
      "customer_segment": "RESIDENTIAL",
      "contact_info": {
        "fixed_svc_phones": ["555-0101"],
        "mobile_svc_phones": [],
        "all_svc_phones": ["555-0101"],
        "contact_phones": ["555-0101"]
      },
      "identification": { "id_type": "NATIONAL-ID", "id_value": "X-00-0000" },
      "services": {
        "fixed_broadband": true,
        "fixed_telephony": true,
        "fixed_tv": false,
        "mobile_telephony": false
      }
    }
  ],
  "metadata": {
    "correlation_id": "auto-gen-3f9c2e1a4b7d4c6e9f0a1b2c3d4e5f60",
    "timestamp": "2026-01-15T14:32:07-05:00",
    "matched_by": "phone_number"
  }
}
```

---

## Record Lookup

Returns the full account/contact record for a single, already-disambiguated
account — the call a client makes after Header Lookup has resolved which
account the interaction is about. Reads the primary cache entry, enriches
with reference data from DynamoDB, and — for the near-real-time variant —
calls an external partner API for a small set of supplemental fields not
carried in the cache. See `src/api/record_lookup/README.md`; no sanitized
handler is included in this repository (out of scope for what this portfolio
covers), so the contract below documents the intended shape rather than
code present in `src/`.

**Endpoint:** `POST /record-lookup` (via the router, `INTENT_NAME: RECORD-LOOKUP`)

### Request fields

| Field        | Type   | Required | Description                                    |
|--------------|--------|----------|--------------------------------------------------|
| `account_id` | string | Yes      | The confirmed account ID from Header Lookup.     |

### Response fields

| Field               | Type    | Description                                                        |
|---------------------|---------|----------------------------------------------------------------------|
| `account`           | object  | Full account record — superset of a Header Lookup `accounts[]` entry, plus billing/reference fields from DynamoDB. |
| `supplemental`      | object  | Fields sourced from the external partner API (near-real-time intent only); omitted when unavailable rather than blocking the response. |
| `metadata`          | object  | Same shape as Header Lookup's `metadata`.                            |

### Error codes

| Status | Meaning                                              |
|--------|---------------------------------------------------------|
| 400    | Missing `account_id`.                                   |
| 404    | `account_id` not found in the primary index.            |
| 500    | Cache, DynamoDB, or partner-API error. Generic message returned to the caller; detail logged server-side only. |

---

## SMS Notification

Validates and dispatches an outbound SMS using a named template with dynamic
field substitution. Rejects destination numbers that are incomplete, fixed-
line, or international per the acceptance criteria in `tests/atp/`. See
`src/api/sms_notification/README.md`; documented here from that service's
intended contract, not from an included handler.

**Endpoint:** `POST /sms-notification` (via the router, `INTENT_NAME: SMS-NOTIF`)

### Request fields

| Field            | Type   | Required | Description                                            |
|------------------|--------|----------|------------------------------------------------------------|
| `destination`    | string | Yes      | Mobile number in national format.                          |
| `template_id`    | string | Yes      | Registered template name.                                   |
| `template_fields`| object | No       | Key/value pairs substituted into the template.              |

### Response fields

| Field           | Type   | Description                                    |
|-----------------|--------|--------------------------------------------------|
| `message_id`    | string | Provider-assigned message ID, once accepted.      |
| `status`        | string | `QUEUED` on success.                              |
| `metadata`      | object | Same shape as Header Lookup's `metadata`.         |

### Error codes

| Status | Meaning                                                                  |
|--------|-----------------------------------------------------------------------------|
| 400    | Missing `destination`/`template_id`, or `destination` fails validation (incomplete, fixed-line, or international number). |
| 422    | `template_id` not recognized, or `template_fields` missing a field the template requires. |
| 500    | Upstream SMS provider error. Generic message returned; detail logged server-side only. |

---

## Places Search

Store/branch locator: category- and market-scoped search against an
external places API, with results run through an internal blacklist filter
before being returned. See `src/api/places_search/README.md`; documented
here from that service's intended contract, not from an included handler.

**Endpoint:** `POST /places-search` (via the router, `INTENT_NAME: PLACES-SEARCH`)

### Request fields

| Field       | Type   | Required | Description                                  |
|-------------|--------|----------|-------------------------------------------------|
| `category`  | string | Yes      | Place category (e.g. `RETAIL-STORE`, `KIOSK`).   |
| `market`    | string | Yes      | Market/opco code to scope the search to.         |
| `latitude`  | number | No       | Caller location, for distance sorting.           |
| `longitude` | number | No       | Caller location, for distance sorting.           |

### Response fields

| Field         | Type   | Description                                             |
|---------------|--------|-------------------------------------------------------------|
| `places[]`    | array  | Matching places, blacklist-filtered, nearest first when a location was supplied. |
| `metadata`    | object | Same shape as Header Lookup's `metadata`.                    |

### Error codes

| Status | Meaning                                                       |
|--------|--------------------------------------------------------------------|
| 400    | Missing `category` or `market`.                                     |
| 500    | External places-API error. Generic message returned; detail logged server-side only. |
