# Header Lookup

Lightweight lookup that returns every account ID / phone number / personal ID associated
with a caller (by phone, account ID, or personal ID), so a client can disambiguate a
multi-account customer before requesting full account detail. Reads directly from the
Subscriber Identity Index primary/secondary Redis indexes — no downstream calls.

See `handler.py` — a sanitized reconstruction of the header-lookup Lambda.
