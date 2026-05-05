# EDMS Simulator — Architecture

## Data flow

```
LOS  ─POST /loans─►  ECS Fargate API ──►  AggregationService
                                            │
                              IdentityResolver  (deterministic SSN → probabilistic name+DOB → new record)
                                            │
                              IncomeAssembler (W2/SE/Rental/SSA/Asset/LES) + CreditAssembler
                                            │
                              ┌─────────────┼──────────────┐
                              ▼             ▼              ▼
                          Aurora      ElastiCache       (S3 documents)
                          (durable)    (4h hot)
```

Async pipelines (SQS FIFO → Lambda):

- `identity-resolution-queue.fifo`  → `identity_resolver_fn`
- `income-assembly-queue.fifo`       → `income_assembler_fn`
- `document-ingestion-queue.fifo`    → `document_ingest_fn`

## Component boundaries

| Module | Responsibility |
|--------|----------------|
| `core/aggregation/service.py` | Central orchestrator, status transitions, event publishing |
| `core/aggregation/status.py`  | GoldenRecord state machine |
| `core/identity/resolver.py`   | 3-strategy match (deterministic / probabilistic / new) |
| `core/identity/golden_record.py` | Pydantic model + SSN hashing + applicant ID generator |
| `core/income/rules.py`        | One function per GSE income type |
| `core/income/assembler.py`    | Per-borrower assembly + lineage hash |
| `core/credit/parser.py`       | Bureau-pull normalizer |
| `core/credit/assembler.py`    | Synthetic profile generator (band-driven) |
| `core/storage/db.py`          | asyncpg pool to Aurora (RDS Proxy) |
| `core/storage/postgres_store.py` | Versioned domain writes (income `superseded_by` chain) |
| `core/storage/redis_store.py` | TTL-keyed hot cache |
| `core/storage/s3_client.py`   | Versioned + KMS document store (local FS in dev) |
| `core/storage/secrets.py`     | Secrets Manager → env var fallback |
| `core/pipelines/*`            | Wiring for the three async pipelines |
| `core/lambda_handlers/*`      | AWS Lambda entry points |
| `api/`                        | FastAPI app, middleware, routes, health |

## Status state machine

```
PLACEHOLDER ─► RESOLVING ─► ACTIVE ─► STALE ─► ACTIVE
       │           │          │          │
       └─► ERROR ◄─┴─► CONFLICT ─► ACTIVE ┘
```

Enforced by `core/aggregation/status.py:StatusMachine.transition`.

## Income profile versioning

Each new assembly inserts a new row in `income_profiles` and updates the prior current row's `superseded_by` to the new `profile_id`. Reads filter `WHERE superseded_by IS NULL`.

## Cache TTLs

| Key                    | TTL       |
|------------------------|-----------|
| `income:{applicant}`   | 4 h       |
| `credit:{applicant}`   | 4 h       |
| `status:{applicant}`   | 24 h      |
| `app_los:{los_id}`     | 12 h      |

## Operational safeguards

- Aurora cluster: `DeletionProtection: true`, `BackupRetention: 7`
- S3 documents bucket: `Retain` deletion policy + Versioning + SSE-KMS
- SQS FIFO: per-queue DLQ at maxReceiveCount=5, 14-day retention
- ECS service: 50/200 deployment band, ALB health-check `/health`
- CloudWatch alarms on CPU/Mem/5xx
