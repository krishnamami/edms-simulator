# EDMS Simulator — Session Context

Operational notes for picking up work mid-stream. Captures architecture deltas
not yet in `docs/ARCHITECTURE.md`, gotchas discovered while building, and a
log of what each phase commit shipped.

---

## Quick start

```bash
# 1. start postgres + redis (ports 5433 + 6380 to avoid host clashes)
docker compose up -d postgres redis

# 2. python deps (one venv, locked in requirements.txt)
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt -r requirements-dev.txt

# 3. env (copy + fill ANTHROPIC_API_KEY if you want chat/image/email body)
cp .env.example .env

# 4. run the API. .env vars must be in the *process* env — the app uses
#    os.getenv() directly (no python-dotenv autoload). On bash:
set -a; source <(grep -v '^#' .env | grep -v '^$' | sed 's/^/export /'); set +a
.venv/Scripts/python -m uvicorn api.main:app --port 8001

# 5. exercise the full pipeline end-to-end
python scripts/simulate_local.py
```

If you skip step 4's env-source step, the app connects to `localhost:5432`
and Redis defaults — both wrong. The Postgres compose port is **5433**, the
Redis port is **6380**.

---

## Repo state at end of last session

- Branch: `main`, all committed and pushed to `https://github.com/krishnamami/edms-simulator`
- Tests: **122 passing, 2 skipped** (live-API tests gated on `ANTHROPIC_API_KEY`)
- `simulate_local.py` runs end-to-end with exit 0 on a clean DB

Latest 5 commits (top of `main`):

```
e1fa6b6  feat(ingestion): Phase E — resilience for upstream Claude failures
bff35cc  feat(simulator): Phase D — 7-step walkthrough exercising all channels
0d0b985  feat(ingestion): Phase C — chat, image, email, pdf, form, csv, xml adapters
08ee2e9  feat(documents): Phase B — generators + pymupdf extractor (no Claude)
75e3a46  feat(ingestion): Phase A — universal ingestion plumbing + persistence fixes
```

---

## Phase log

| Phase | Commit | Scope |
|-------|--------|-------|
| **A** | `75e3a46` | Universal ingestion plumbing — `core/ingestion/{events,router,confidence}.py`, `api_adapter`, `_handle_normalized_ingest_event`, `/ingest/*` endpoints stubbed (501-ish). Bundled persistence fixes that closed 3 simulator gaps. |
| **B** | `08ee2e9` | Document **generators** (W2 / paystub / bank stmt / credit report / driver's license JPG) and **extractors** (`pymupdf_extractor`). `claude_extractor` placed as a stub for Phase C wiring. Round-trip tests prove gen→extract→assert. |
| **C** | `0d0b985` | All 7 channel adapters (chat / image / email / pdf / form / csv / xml). Anthropic SDK wired via shared `_claude_client.py` (model `claude-sonnet-4-6`, prompt-caching on system block). Adapters injectable for tests. `/ingest/*` endpoints replaced stubs with real implementations. |
| **D** | `bff35cc` | `scripts/simulate_local.py` rewritten — 7-step walkthrough exercising every channel + verifying golden record / Redis / Postgres / xref. |
| **E** | `e1fa6b6` | Resilience for upstream Claude errors. `email_adapter` body-extract falls back gracefully (attachments still process). `/ingest/{chat,image,email}` map `anthropic.APIStatusError` → HTTP 502 with detail. Simulator distinguishes **failed (live)** vs **skipped (no key)**. |

---

## Persistence fixes that landed during Phase A

These were latent bugs surfaced by `simulate_local.py`:

1. **FK violation** on `applications.applicant_id` — service.py never wrote the
   golden record to Postgres before saving the application. Fix: persist
   `gr.identity_xrefs` and `model_dump()` of the golden record before
   `save_application`.
2. **asyncpg date binding** — passing ISO strings to `date` / `timestamptz`
   columns failed even with `::date` casts (cast helps SELECT, not INSERT).
   Added `_to_date` / `_to_ts` helpers in `postgres_store.py`.
3. **Schema unique-constraint bug** — `applicant_identity_xref UNIQUE
   (source_system, source_id)` blocked joint applications (primary +
   co-borrower share one LOS-ID). Changed to `(applicant_id, source_system,
   source_id)`. Live ALTER + schema.sql edit + ON CONFLICT update.
4. **GET response shape mismatch** — `simulate_local.py` reads
   `data["source"]` and `data["data"]`. `IncomeProfileResponse` /
   `CreditProfileResponse` now expose both alongside legacy `profile`/`cached`.
5. **Missing `/loans/document` alias** — simulator step 4 calls a path that
   didn't exist; added as alias for `/documents/upload`.

Also fixed: `S3Client.upload_document` accepts `extension` + `content_type`
so the JPG driver's license gets the right key suffix and mime type.

---

## Universal ingestion architecture (Phase A–E delta on top of existing ARCHITECTURE.md)

```
caller payload
       │
       ▼
IngestRouter.detect_channel()   ── content-based sniffing (PDF magic bytes,
       │                          JFIF/PNG/TIFF headers, XML <?xml,
       ▼                          chat = list[{role,content}], etc.)
ChannelType   ──► IngestRouter.route() ──► adapter
                                              │
                            ┌─────────────────┼─────────────────┐
                            ▼                 ▼                 ▼
                       deterministic     Claude-based       hybrid
                       (api/pdf/form/    (chat/image/       (email = body
                        csv/xml)          email body)        Claude + attachments
                                                             via pdf/image)
                            │
                            ▼
                NormalizedIngestEvent  (shared shape — channel-agnostic)
                            │
                            ▼
                AggregationService.handle()
                    ├── API channel: maps to ApplicationSubmittedEvent (full pipeline)
                    └── all other channels: NotImplementedError (BUILD 12 deferred)
```

`SOURCE_CONFIDENCE_RANKING` (in `core/ingestion/confidence.py`) ranks
sources for `ConfidenceResolver`:

```
IRS_TRANSCRIPT  0.99   PAYROLL_API  0.97   W2_PDF       0.95
PAYSTUB_PDF     0.93   BANK_STMT_PDF 0.90   FORM_1040_PDF 0.90
API_JSON        0.88   WEB_FORM     0.85   CHAT          0.80
EMAIL_BODY      0.75   VERBAL_STATED 0.50
```

Conflicts flagged when numeric values diverge >10% across sources.

### Endpoint map

| Path                       | Channel       | Claude needed |
|----------------------------|---------------|---------------|
| `POST /loans`              | API           | no            |
| `POST /loans/document`     | (alias)       | no            |
| `POST /documents/upload`   | DOCUMENT_UPLOADED | no       |
| `POST /ingest/pdf`         | PDF_UPLOAD    | optional fallback |
| `POST /ingest/image`       | IMAGE_UPLOAD  | yes (vision) |
| `POST /ingest/email`       | EMAIL         | optional (body); attachments don't need it |
| `POST /ingest/chat`        | CHAT          | yes |
| `POST /ingest/form`        | FORM          | no |
| `POST /ingest/csv`         | CSV_BATCH     | no |
| `POST /ingest/xml`         | XML           | no |

Errors:
- `503` — `ClaudeUnavailable` (no API key)
- `502` — Anthropic upstream error (e.g. quota), with the upstream message in `detail`
- `500` — anything else (unhandled)

---

## Open follow-ups (latent, not blocked on the above)

1. **`XRefStore` is in-memory** — wiped on uvicorn restart while Postgres
   persists. After a restart, the resolver can create a fresh `applicant_id`
   for an SSN that already exists in DB → `idx_applicant_ssn` UniqueViolation.
   Workaround in dev: truncate Postgres before each fresh run. **Real fix:**
   hydrate XRefStore from Postgres at startup, or have the resolver
   fall back to a Postgres lookup on cache miss.
2. **`_handle_normalized_ingest_event` only handles API channel** — for chat /
   pdf / etc., the adapter produces a `NormalizedIngestEvent` but the service
   raises `NotImplementedError`. BUILD 12 (full ConfidenceResolver merge into
   the income profile) was deferred. Today the `/ingest/*` endpoints return
   the event without merging into a profile.
3. **`claude_extractor` body** — Phase B placed the file with the documented
   signature; Phase C extension never replaced the `NotImplementedError` body
   (the pdf_adapter's `claude_fallback` path catches it gracefully). Implement
   when there's a real document type that pymupdf can't handle.
4. **`/ingest/csv` doesn't ingest** — the endpoint returns the report and
   parsed signals but doesn't drive applicants into Postgres. Wire each event
   through the aggregation service when BUILD 12 is done.

---

## Common pitfalls (encountered & fixed)

- **Bash sessions don't persist between tool calls** — env vars exported in
  one Bash call are gone in the next. When running multi-step shell flows,
  bundle them in one command or re-source `.env` each time.
- **Windows console uses cp1252** — `simulate_local.py` uses Unicode
  box-drawing chars; set `PYTHONIOENCODING=utf-8` or get
  `UnicodeEncodeError`.
- **reportlab stamps a CreationDate** — same RNG seed → identical content,
  but PDF bytes differ. Compare metadata, not bytes, for determinism tests.
- **`python-multipart` is required** for the `UploadFile` / `Form`
  endpoints. It's pinned in `requirements.txt` now (Phase A).
- **`fakeredis` is required** for the test suite (the `RedisStore` import
  hits `USE_FAKE_REDIS=true` in `tests/conftest.py`). It's in
  `requirements-dev.txt`.
- **Anthropic credit balance** — surfaces as `BadRequestError` with code 400.
  Phase E maps that to HTTP 502 with the upstream message; the email pipeline
  no longer breaks on it.

---

## Local env (.env keys)

| Var | Where used | Notes |
|-----|------------|-------|
| `ENVIRONMENT` | `local` flag | sometimes branches behavior |
| `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD` | `core/storage/db.py` | port **5433** locally |
| `REDIS_HOST/REDIS_PORT/REDIS_PASSWORD/REDIS_SSL` | `core/storage/redis_store.py` | port **6380** locally |
| `USE_LOCAL_STORAGE` | `core/storage/s3_client.py` | `true` writes to `local_storage/` |
| `LOCAL_STORAGE_PATH` | `s3_client.py` | default `./local_storage` |
| `USE_AWS_SECRETS` / `USE_AWS_SQS` / `USE_FAKE_REDIS` | various | gates AWS-vs-local backends |
| `AWS_REGION` / `AWS_S3_BUCKET` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | aws clients | unused in dev |
| `API_KEY` | `api/routes.py:verify_api_key` | header `X-API-Key` |
| `DECISION_OS_URL` | downstream call placeholder |  |
| `ANTHROPIC_API_KEY` | `core/ingestion/_claude_client.py` | required for chat / image / email body. Account-level errors are fine — Phase E handles them. |

---

## Testing strategy

- **Unit tests** in `tests/core/**` — pure deterministic, no network.
- **Round-trip tests** in `tests/core/documents/` — generator → extractor →
  assert metadata recovered.
- **Mocked Claude tests** in `tests/core/ingestion/test_chat_adapter.py`
  (and friends) — `tests/core/ingestion/_fakes.py::FakeClaudeClient` returns
  canned JSON. Adapters take `client=` so tests inject without env.
- **Live tests** gated on `ANTHROPIC_API_KEY` via `pytest.mark.skipif`.
  Set the key locally (and have credit) to exercise them. Two such tests
  exist: `test_chat_adapter::test_live_chat_extraction_returns_valid_event`
  and `test_claude_extractor::test_extract_phase_c_implementation_pending`.

```bash
# normal run (skips live)
.venv/Scripts/python -m pytest tests/ -q

# include live tests (will spend Anthropic credits)
set -a; source <(grep -v '^#' .env | grep -v '^$' | sed 's/^/export /'); set +a
.venv/Scripts/python -m pytest tests/ -q
```

---

## Helpful one-liners

```bash
# truncate everything for a clean simulate run
docker exec edms-simulator-postgres-1 psql -U edms -d edms -c \
  "TRUNCATE applicants, applicant_identity_xref, applications, income_profiles, credit_profiles, document_index RESTART IDENTITY CASCADE;"
docker exec edms-simulator-redis-1 redis-cli FLUSHDB

# tail the API log
tail -f .logs/uvicorn.log

# inspect a Redis key
docker exec edms-simulator-redis-1 redis-cli GET "income:APL-00001-P"

# list documents written by S3Client local fallback
ls -R local_storage/
```
