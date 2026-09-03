# Record Lookup

Full account/contact record service. Reads the primary cache entry for the confirmed
account, enriches with DynamoDB reference data, and — for the near-real-time intent —
calls an external partner API for supplemental fields.

Not included in this repository — the code sample in this portfolio focuses on the Subscriber Identity Index pipeline and the Header Lookup API (see `src/etl/subscriber_identity_index/` and `src/api/header_lookup/`). This service's contract is documented in `docs/api-reference.md`.
