# EDMS Simulator API

Base URL: `http://<host>:8001`
Auth: `X-API-Key: <key>` (validated against `edms/api/keys` secret in production)

## POST /loans

Submit a new loan application — triggers identity resolution + income/credit assembly.

Request:

```json
{
  "los_id": "LOS-001",
  "borrower": {
    "first_name": "John",
    "last_name": "Doe",
    "dob": "1980-01-15",
    "ssn_hash": "<sha256>",
    "ssn_last4": "6789",
    "email": "john@example.com"
  },
  "loan": { "credit_band": "near-prime" },
  "documents": [
    { "document_id": "DOC-001", "document_type": "W2", "borrower_role": "primary",
      "box1_wages": 96000, "employer_name": "Acme" }
  ]
}
```

Response:

```json
{
  "application_id": "APP-LOS-001",
  "applicant_id": "APL-00001-P",
  "co_applicant_id": null,
  "status": "active",
  "match_method": "new_record",
  "is_new_record": true
}
```

## GET /loan/{los_id}/applicant-id

Resolve LOS identifier to internal applicant + application IDs. Redis cache → Postgres.

## GET /applicant/{applicant_id}/income-profile

Returns the latest active `IncomeProfile`.

## GET /applicant/{applicant_id}/credit-profile

Returns the current `CreditProfile`.

## POST /documents/upload

Body:

```json
{
  "applicant_id": "APL-00001-P",
  "application_id": "APP-LOS-001",
  "all_documents": [ ... ]
}
```

Triggers re-assembly: golden record goes `active → stale → active`, income profile is re-versioned (`superseded_by` chain).

## GET /health

Liveness probe — returns `{"status": "ok", "version": "0.1.0"}`.

## GET /ready

Readiness probe — checks Postgres + Redis connectivity:

```json
{ "status": "ok", "postgres": true, "redis": true }
```
