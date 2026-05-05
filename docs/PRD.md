# EDMS Simulator — Product Requirements

## 1. Problem & vision

Mortgage origination data flows through a long, brittle chain: borrower
intake → identity resolution → document extraction → income/credit assembly →
decisioning. Most teams test these stages in isolation against
captured-once fixtures. The EDMS Simulator gives engineers a **runnable
end-to-end environment** that mirrors production AWS infrastructure (Aurora,
ElastiCache, S3, ECS, SQS) and accepts real-shape data through every channel
that production does — JSON API, PDF, image, email, chat, web form, CSV bulk
upload, MISMO/IRS XML.

The vision is "one local stack you can throw any kind of data at and watch
it walk the whole pipeline" — useful for integration testing, decisioning
verification, and shape-discovery before wiring real bureau pulls or third
parties.

## 2. Personas

| Persona | What they need from the simulator |
|---------|-----------------------------------|
| **Platform engineer** integrating with Decision OS | A stable mock that returns the same shapes prod will return, plus knobs to inject conflict/edge cases. |
| **ML / extraction engineer** | A way to drop a PDF or chat transcript in and see what the extraction → confidence → assembly pipeline does with it. |
| **Underwriting / credit team** | Confidence scoring + conflict flags so they can audit which source drove a decision. |
| **Architect on call** | A fast local repro for production incidents (FK violations, schema drift, async-pool issues). |

## 3. Capabilities

### Shipped (✓)

| Area | Capability |
|---|---|
| Universal ingestion | `IngestRouter` content-detects payloads (`%PDF`, JFIF/PNG/TIFF, XML, list-of-chat-messages, etc.) and dispatches to the right adapter. |
| Channel adapters | API · PDF · Image · Email · Chat · Form · CSV · XML — all wired through `POST /ingest/*` endpoints. |
| Identity resolution | 3-strategy match (deterministic SSN-hash → probabilistic name+DOB → new record) with persistent xrefs in `applicant_identity_xref`. |
| Income assembly | Per-borrower assembler covering W2 / self-employed / rental / SSA / asset depletion / military, with versioned `income_profiles` (`superseded_by` chain). |
| Credit assembly | Synthetic tri-merge profile generator deterministic by `credit_band`. |
| Document generation | reportlab-rendered W2, paystub, bank statement, credit report; Pillow-rendered driver's license JPG; `package_generator` assembles full doc sets. |
| Document extraction | PyMuPDF text extractor with type-detection + confidence scoring. Claude vision extractor stubbed for plug-in. |
| Confidence ranking | `SOURCE_CONFIDENCE_RANKING` (IRS=0.99 … VERBAL=0.50). `ConfidenceResolver` picks the highest-confidence value across sources and flags >10% numeric divergence as a conflict. |
| Caching | Redis (TTL-keyed) for status / income / credit / app-lookup. |
| Persistence | Aurora-Postgres (asyncpg pool). FK-safe write order, JSONB columns, idempotent upserts. |
| API | FastAPI app, `X-API-Key` auth, `/health` + `/ready`, structured-log middleware, all `/ingest/*` and `/loans*` endpoints. |
| Resilience | Anthropic upstream errors map to HTTP 502 with detail; email body fallback preserves attachment processing. |
| Walkthrough | `scripts/simulate_local.py` runs all 7 ingestion-+aggregation steps end-to-end. |

### Pending (⏳)

| Area | What's left |
|---|---|
| `_handle_normalized_ingest_event` for non-API channels | Today, only API events drive the full aggregation pipeline. Chat/PDF/email events are produced but not merged into the profile via `ConfidenceResolver`. (Spec called this BUILD 12.) |
| `claude_extractor.extract` body | Stub raises `NotImplementedError`. The pdf_adapter handles this gracefully today. Real implementation lands when a use case demands it. |
| XRefStore startup hydration | After uvicorn restart, in-memory state is empty while Postgres persists. Causes `idx_applicant_ssn` UniqueViolation on re-run with existing SSN. Fix: hydrate from Postgres at startup, or have resolver fall back to a Postgres lookup. |
| `/ingest/csv` ingestion | Endpoint returns parsed signals + report; doesn't push events through the aggregation pipeline. |
| Live Claude extractor for image | Today image_adapter calls Claude vision directly; could route through `claude_extractor` for unified retry/cache. |

### Non-goals

- Production-grade rate limiting / authn / authz beyond the dev `X-API-Key`.
- A UI. The simulator is API + scripts only.
- Real bureau-pull integrations (Experian / Equifax / TransUnion APIs). The
  `CreditAssembler` synthesizes a plausible profile from the requested band.
- A queue / async worker substitute. Local runs are synchronous; the AWS
  Lambda + SQS code lives in `core/lambda_handlers/` and `core/pipelines/`
  for production wiring but isn't exercised locally.

## 4. Functional requirements

| FR | Requirement |
|----|-------------|
| FR-1 | Each channel adapter MUST emit a `NormalizedIngestEvent` matching the schema in `core/ingestion/events.py`. |
| FR-2 | `IngestRouter.detect_channel` MUST classify a payload from content alone — no caller-supplied content-type. |
| FR-3 | Identity resolution MUST persist the golden record to Postgres before any application row that references it (FK ordering). |
| FR-4 | `applicant_identity_xref` MUST allow primary + co-borrower to share a `(source_system, source_id)` pair. |
| FR-5 | The pymupdf extractor MUST recover ≥85% of expected fields for each document type produced by the matching generator (W2, paystub, bank statement, credit report). |
| FR-6 | All chat-extracted values MUST be flagged `requires_verification=True`. |
| FR-7 | When a Claude API call fails with an account-level error, the email pipeline MUST still produce attachment events. |
| FR-8 | Anthropic upstream errors MUST surface to API callers as HTTP 502 with the upstream `detail`, not as opaque 500s. |
| FR-9 | Tests dependent on Claude MUST be skipped (not failed) when `ANTHROPIC_API_KEY` is absent. |
| FR-10 | `simulate_local.py` MUST run to completion (exit 0) without `ANTHROPIC_API_KEY`, exercising the deterministic 5/7 steps. |

## 5. Non-functional requirements

| NFR | Requirement |
|-----|-------------|
| Performance | CSV adapter MUST process 100 rows in < 2 seconds. Local API median latency for `/loans` < 200ms on a clean DB. |
| Reproducibility | Document generators MUST be deterministic given the same seed (content equality; PDF bytes differ because reportlab stamps a CreationDate). |
| Observability | All API requests log a single JSON line via `RequestMiddleware` including `request_id`, `method`, `path`, `status_code`, `elapsed_ms`. |
| Test coverage | New adapters require both a deterministic test (mock or content fixture) and, where applicable, a key-gated live test. |
| Safety | Local docker compose ports differ from defaults (5433 / 6380) so the dev stack can't clash with system Postgres / Redis. |

## 6. Acceptance criteria

The simulator is considered "running correctly" when:

1. `docker compose up -d postgres redis` brings both services healthy.
2. `python -m pytest tests/ -q` reports `122 passed, 2 skipped` (live tests
   skipped without API key) on a fresh checkout.
3. `python scripts/simulate_local.py` exits 0 and produces:
   - 5 documents in `local_storage/demo/` (≈ 51 KB total)
   - HTTP 200 from `/ingest/pdf` for the W2 with `confidence=1.0`
   - HTTP 200 from `/ingest/email` with both body and attachment events
   - HTTP 200 from `/loans` with a fresh `applicant_id`
   - 2 rows in `applications` and 4 in `applicant_identity_xref` after
     STEP 6 (deterministic match)
4. With `ANTHROPIC_API_KEY` set + sufficient credit, STEP 1 chat extraction
   returns structured borrower / co-borrower / income-source data.
5. With `ANTHROPIC_API_KEY` set but no credit, the simulator surfaces the
   upstream error message clearly and continues; STEP 3 attachment still
   processes at confidence 1.0.

## 7. Dependencies & constraints

- **Runtime**: Python 3.12, Docker Desktop. Tested on Windows 11.
- **Locked Python deps** in `requirements.txt` (runtime) and
  `requirements-dev.txt` (test). Notable additions during build-out:
  - `python-multipart` (for `UploadFile` / `Form`)
  - `reportlab`, `PyMuPDF`, `Pillow` (Phase B)
  - `anthropic>=0.99.0` (Phase C)
- **External services**: optional Anthropic API for chat/image/email body.
  Cost ≈ a few cents per simulate_local run when live; deterministic adapters
  are free.
- **AWS**: production code targets Aurora + ElastiCache + S3 + ECS + SQS.
  Local stack uses Postgres 15 + Redis 7 in containers and `local_storage/`
  for documents.

## 8. Risks

| Risk | Mitigation |
|------|-----------|
| Anthropic API key leaks via logs | Adapters never log the key; `_claude_client` reads env each call (no print). `.env` is gitignored. |
| Schema drift between `infra/schema.sql` and a running DB | The Phase A side fix (xref unique constraint) updated both. Future schema changes should land in both places + add a migration step. |
| Confidence ranking diverges from real underwriting truth | The numbers in `SOURCE_CONFIDENCE_RANKING` are spec-driven; underwriting can override per-deployment by editing the dict — it's the only source. |
| `XRefStore` in-memory state diverges from Postgres | Documented in `context.md`. Workaround: truncate before each fresh demo. Real fix: hydrate from Postgres at startup. |

## 9. References

- Local architecture: `docs/ARCHITECTURE.md`
- API surface: `docs/API.md`
- Session-resume notes & gotchas: `context.md`
- AWS deployment: `infra/cfn-template.yaml` + `.github/workflows/aws.yaml`
