# Router

Single API Gateway entry point. Inspects `INTENT_NAME` (and `MARKET` where relevant) on
the incoming request and dispatches synchronously to the matching downstream Lambda
(header_lookup, record_lookup, sms_notification, places_search). Keeps a single public
endpoint while allowing each downstream service to scale, deploy, and fail independently.

See `handler.py` — a sanitized, scoped-down reconstruction of the router (the
original also dispatches to several unrelated internal systems, omitted here
since they're out of scope for this platform).
