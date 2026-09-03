# Subscriber Identity Index Design Notes

## Problem statement

Contact-center and IVR/chat channels identify a caller and hand back account
context before a conversation can proceed. The original design could only
resolve the *first* account associated with an inbound identifier (phone
number, account ID, or personal ID) — if a customer had more than one
account and the query was about a different one, the near-real-time lookup
returned an incomplete or wrong result rather than timing out gracefully or
surfacing the ambiguity.

## Root cause

No design documentation existed for the legacy key scheme, so the fix started
with reverse-engineering actual production behavior against real traffic
patterns. That surfaced the underlying issue: the cache was keyed by an
account-level "contact phone" field — a number a customer provides when
opening an account, for reachability purposes — rather than by the phone
number actually associated with a telephony *service*. Contact phones are
**not unique**: the same number is routinely shared across a household's
multiple accounts, so keying on it collapses distinct customers into a single
cache entry and whichever account happened to load last wins.

Service phone numbers (the MSISDN tied to an actual fixed or mobile telephony
line) are a much better key candidate — but not a complete one, since
broadband- or TV-only fixed accounts have no telephony service and therefore
no service phone at all. Any design has to account for that gap rather than
assume every account has one clean unique phone.

## Design

**Sources.** Three daily inputs feed the Subscriber Identity Index: the billing/CRM
convergent account and service tables (data lake), a voice/IVR export
(caller-ID-to-account association), and a chat-platform export (same
association, chat channel). An optional manual-insert path exists for
contact-center agents to link an identifier to an account directly when
automated matching fails.

**Key structure.** Account ID is the primary key, holding the full account
record as a JSON value. Phone number and personal ID are **secondary
indexes** — each maps to a *set* of account IDs, not a single account —
because both can legitimately be associated with more than one account (a
shared contact number; a personal ID tied to both a personal and a business
account, for example). This is the structural fix: uniqueness is enforced at
the account-ID level, and everything else is treated as a many-to-many
association resolved at query time.

**Query flow.** A client (IVR, chat bot, live agent tooling) calls the header
endpoint with whatever identifier it has (typically an ANI/caller ID). If the
identifier resolves, the response lists every account associated with it; the
client then prompts the caller to confirm which account the interaction is
about (or auto-selects when there's only one) before calling the full-record
endpoint with the confirmed account ID. If the identifier doesn't resolve, or
the caller says the match is wrong, the flow falls back to asking for an
account ID, phone number, or personal ID directly and retries the header
call. This header/detail split is what makes multi-account resolution
work in near real time: the expensive part (full account enrichment) only
runs once ambiguity is resolved, not once per candidate account.

## Redis TTL and secondary-index staleness

A key/value store's per-key TTL doesn't compose cleanly with secondary
indexes structured as sets. If phone number `X` is linked to accounts `A` and
`B` today, and next month `B` is no longer associated with `X`, TTL alone
won't remove `B` from the set — TTL expires the whole key `phone:X`, not
individual members. Left alone, stale members accumulate until the entire
key happens to expire, which can take up to the full TTL window.

Two approaches were considered:

- **Full refresh** — rebuild every index from scratch each day. Simple, but
  means the whole cache is only ever as fresh as the most recent 24-hour
  batch, with no cheaper way to reconcile smaller day-to-day changes.
- **Diff-style incremental update** — compare each day's dataset against the
  previous cache state and write only what changed: new keys, changed
  values, and — critically — explicit removal of stale set members via
  `SREM` rather than waiting on TTL. This was implemented, tested, and
  adopted; it's the strategy `src/etl/subscriber_identity_index/load_job.py` in this
  repository implements.

Diff-style load logic, in short: a primary-index key gets created with a
fresh TTL if absent, TTL-refreshed if unchanged, or overwritten with a fresh
TTL if changed. A secondary-index set gets created if new, left alone to
expire naturally if entirely absent from today's data, TTL-refreshed if
membership is unchanged, or has members explicitly added/removed to match
today's data (with a TTL refresh) if membership differs.

## API contract

The lookup API accepts a request containing at least one of account ID,
phone number, or personal ID (checked in that priority order), and returns
every account associated with whichever identifier resolved, tagged with
which identifier type produced the match (`matched_by`) so the calling
client can decide how much to trust the result — see
[`api-reference.md`](api-reference.md) for the full request/response shapes.
