# Acceptance Test Procedures (ATP)

This directory documents the sanitized testing approach; production ATP workbooks,
request/response captures, and environment-specific results are intentionally omitted.

The delivery used a structured matrix per API service (test ID, description, input
payload, expected result, actual result, pass/fail), covering happy paths and negative
paths such as malformed payloads, missing required fields, and invalid destination
formats.

Each service's ATP is a spreadsheet or table with these columns: Test Case ID,
Test Description, Test Input Payload, Expected Result, Test Date, Test Output
Payload, Test Result (pass/fail), Comments. Use entirely synthetic identifiers
in every payload — a real ATP run captures actual request/response pairs
against a live environment, which for these services can include account IDs,
personal IDs, phone numbers, addresses, internal routing behavior, and third-party
responses. None of that belongs in a public repository, so any future executable
tests here must use fabricated identifiers and mocked or sandbox integrations from
the start rather than sanitized production captures.

Retained source records document 16 designed Places Search scenarios and 21 SMS
Notification scenarios. A Places Search execution snapshot recorded 12 executed,
10 passed, and 2 failed in its summary; this repository does not present those
results as a complete passing suite.
