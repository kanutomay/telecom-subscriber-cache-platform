# Fixed LOB ETL

Fixed-line subscriber pipeline. Extract job joins the data lake with a frequent-caller
priority file; load job performs a blue/green (staging-DB → validate → SWAPDB) cache
reload with automatic revert on failed validation.

Not included in this repository — the code sample in this portfolio focuses on the Subscriber Identity Index pipeline and the Header Lookup API (see `src/etl/subscriber_identity_index/` and `src/api/header_lookup/`). This module is documented at the architecture level only.
