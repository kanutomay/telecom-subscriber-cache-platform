# Subscriber Identity Index ETL

Cross-LOB pipeline. Extract job joins the billing/CRM data lake with daily IVR and chat
export files, builds the primary (account ID) and secondary (phone, personal ID) indexes,
and writes Parquet to staging. Load job performs the diff-style incremental update into
Redis/Valkey (new keys get a fresh TTL, unchanged keys are TTL-refreshed, changed keys are
overwritten) with a post-load validation-sampling step.

See `load_job.py` — a sanitized reconstruction of the diff-style load job
(the flagship sample in this repo). The extraction job and pre-run-check
Lambda that feed it are not included in this repository; this module's
code sample focuses on the load step.
