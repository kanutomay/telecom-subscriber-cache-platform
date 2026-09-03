# Step Functions

This directory documents the intended orchestration pattern; it does not contain
deployable Amazon States Language (ASL) definitions.

The production design used one state machine per pipeline (`subscriber_identity_index`, `fixed_lob`,
`postpaid_lob`, and `prepaid_lob`) following this general sequence:

1. Run a pre-check Lambda with bounded retry/backoff to confirm upstream partitions
   and basic data readiness.
2. Run the extraction/transformation Glue job.
3. Refresh the relevant Glue Catalog table through a crawler where required.
4. Run the cache load Glue job.
5. Publish success or failure through SNS.

The production definitions and resource identifiers are intentionally omitted. A
new implementation must supply its own IAM policies, retry/catch behavior, job and
crawler names, networking, timeouts, and notification topic.
