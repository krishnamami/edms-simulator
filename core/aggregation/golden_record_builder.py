"""Golden-record rebuild orchestrator.

Builds the five "golden record" tables for ONE application from the
already-indexed raw data:

  - ``income_profiles``        — per applicant
  - ``credit_profiles``        — per applicant
  - ``applicant_identity_xref`` — per applicant per source_document_id
  - ``entity_states``          — one row per application (the v4 shape:
                                  borrower + co_borrowers + property +
                                  loan_terms as JSONB, plus indexed cols
                                  for mid_score / qualifying_monthly /
                                  loan_amount / dti / piti / ltv / etc.)
  - ``entity_state_events``    — change log row

Two entry points:

  - ``rebuild_one(pg, redis, application_id, tenant_id)`` — full rebuild;
    re-runs the income + credit assemblers against the docs currently in
    ``document_index``, then composes the entity_states row. Used by the
    POST /admin/rebuild-golden-records backfill loop AND by the regular
    upload flow after every ``_run_assembly`` so future uploads keep
    the golden record current.

  - ``read_backfill_state(pg, tenant_id)`` / ``write_backfill_state(...)``
    — singleton watermark helpers backing the restartable backfill loop.
    On crash, the next POST resumes from
    ``last_completed_application_id`` instead of starting over.

Why this module exists:
  ``core/aggregation/entity_state_builder.py:upsert_all_entities`` was
  shipped with a signature mismatch — it calls
  ``pg.upsert_entity_state(entity_id=, entity_type=, …)`` but the
  actual PG method takes ``(application_id, state_data, tenant_id)``.
  Every call falls into the per-entity ``except Exception`` clause and
  logs a warning, which is why ``entity_states`` was empty in prod even
  with 261k docs in ``document_index``. This module bypasses that
  broken orchestrator and writes the row the schema actually wants.

Every PG write here is idempotent:
  - ``save_income_profile`` / ``save_credit_profile`` are DELETE-then-
    INSERT keyed on ``applicant_id`` — re-running an application
    overwrites the prior row.
  - ``save_xref`` is ``ON CONFLICT (applicant_id, source_system,
    source_id) DO NOTHING`` — safe to re-run.
  - ``upsert_entity_state`` is ``ON CONFLICT (application_id) DO
    UPDATE`` — full row replacement except ``legacy_ids`` which JSONB-
    merges.
  - ``log_entity_state_event`` is plain INSERT. Backfill events use a
    single ``event_type='golden_record_rebuilt'`` per application so
    the log doesn't accumulate per-field churn from a re-run.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from core.storage import db

logger = logging.getLogger(__name__)


# ----- Watermark helpers -----------------------------------------------------

_BACKFILL_TABLE = "golden_record_backfill_state"


async def read_backfill_state(tenant_id: str = "default") -> dict:
    """Return the singleton row for this tenant (or a fresh default if
    no row exists yet — first call after schema migration)."""
    row = await db.fetchrow(
        f"SELECT * FROM {_BACKFILL_TABLE} WHERE tenant_id = $1",
        tenant_id,
    )
    if not row:
        return {
            "tenant_id":                     tenant_id,
            "last_completed_application_id": None,
            "completed_count":               0,
            "total_count":                   0,
            "status":                        "not_started",
            "errors":                        [],
            "started_at":                    None,
            "updated_at":                    None,
            "completed_at":                  None,
        }
    out = dict(row)
    errs = out.get("errors")
    if isinstance(errs, str):
        try:
            out["errors"] = json.loads(errs)
        except Exception:
            out["errors"] = []
    return out


async def write_backfill_state(state: dict, tenant_id: str = "default") -> None:
    """UPSERT the singleton row. Always overwrites — caller is
    responsible for merging deltas (typically: read → modify → write
    in the orchestrator loop)."""
    errs = state.get("errors") or []
    await db.execute(
        f"""
        INSERT INTO {_BACKFILL_TABLE} (
            tenant_id, last_completed_application_id,
            completed_count, total_count, status, errors,
            started_at, updated_at, completed_at
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, NOW(), $8)
        ON CONFLICT (tenant_id) DO UPDATE SET
            last_completed_application_id = EXCLUDED.last_completed_application_id,
            completed_count               = EXCLUDED.completed_count,
            total_count                   = EXCLUDED.total_count,
            status                        = EXCLUDED.status,
            errors                        = EXCLUDED.errors,
            started_at                    = COALESCE(EXCLUDED.started_at,
                                                     {_BACKFILL_TABLE}.started_at),
            updated_at                    = NOW(),
            completed_at                  = EXCLUDED.completed_at
        """,
        tenant_id,
        state.get("last_completed_application_id"),
        int(state.get("completed_count") or 0),
        int(state.get("total_count") or 0),
        state.get("status") or "running",
        json.dumps(errs, default=str),
        state.get("started_at"),
        state.get("completed_at"),
    )


# ----- Application iteration -------------------------------------------------

async def iter_application_ids_for_backfill(
    tenant_id: str,
    *,
    after_application_id: Optional[str] = None,
    prefetch: int = 500,
) -> AsyncIterator[str]:
    """Yield every application_id for this tenant in deterministic
    ascending order, starting AFTER ``after_application_id`` if given.
    Backed by an asyncpg server-side cursor (``db.stream``) so the 9k-
    application corpus doesn't materialize in memory."""
    if after_application_id:
        async for row in db.stream(
            "SELECT application_id FROM applications "
            "WHERE tenant_id = $1 AND application_id > $2 "
            "ORDER BY application_id ASC",
            tenant_id, after_application_id, prefetch=prefetch,
        ):
            yield row["application_id"]
    else:
        async for row in db.stream(
            "SELECT application_id FROM applications "
            "WHERE tenant_id = $1 "
            "ORDER BY application_id ASC",
            tenant_id, prefetch=prefetch,
        ):
            yield row["application_id"]


async def count_applications(tenant_id: str) -> int:
    """How many applications the tenant has — used to populate
    ``total_count`` in the watermark when the backfill starts."""
    row = await db.fetchrow(
        "SELECT COUNT(*)::int AS n FROM applications WHERE tenant_id = $1",
        tenant_id,
    )
    return int(row["n"]) if row else 0


# ----- Per-application rebuild ----------------------------------------------

# Verification flags evaluated against the doc_type set assembled for the
# application. 12 flags total — gives a 0-100% completeness score that
# drives ``status`` (intake / in_progress / complete).
_VERIFICATIONS = [
    "income_verified", "employment_verified", "credit_pulled",
    "assets_verified", "identity_complete", "appraisal_complete",
    "title_clear", "insurance_bound", "aus_approved",
    "rate_locked", "conditions_cleared", "clear_to_close",
]


def _f(v) -> Optional[float]:
    """Coerce JSON-ish to float; None on anything unparseable. Tolerates
    strings like ``"450000"`` that show up in extracted_fields after the
    chaos-test hardening."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace("$", "").replace(",", "")
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _piti(loan_amount, interest_rate_pct, term_months, annual_tax, annual_hoi, mi_monthly) -> Optional[float]:
    """Total monthly PITI: amortized P&I + tax + HOI + MI. Returns None
    if the loan terms are missing."""
    la = _f(loan_amount)
    ir = _f(interest_rate_pct)
    n  = int(term_months or 360)
    if not la or ir is None or n <= 0:
        return None
    r = ir / 100.0 / 12.0
    try:
        if r > 0:
            pi = la * r / (1 - (1 + r) ** -n)
        else:
            pi = la / n
    except (OverflowError, ZeroDivisionError):
        return None
    tax = (_f(annual_tax) or 0) / 12.0
    hoi = (_f(annual_hoi) or 0) / 12.0
    mi  = _f(mi_monthly) or 0
    return pi + tax + hoi + mi


def _mi_pct_by_ltv(ltv) -> float:
    """Mortgage-insurance rate by LTV band (Conv/FHA-ish):
      >95%   → 1.10%
      90-95% → 0.80%
      85-90% → 0.52%
      80-85% → 0.32%
      ≤80%   → 0
    """
    if not ltv:
        return 0.0
    if ltv > 95:   return 0.0110
    if ltv > 90:   return 0.0080
    if ltv > 85:   return 0.0052
    if ltv > 80:   return 0.0032
    return 0.0


def _classify_status(pct: float) -> str:
    if pct >= 90: return "complete"
    if pct >= 50: return "in_progress"
    return "intake"


async def _save_xrefs_for_docs(pg, docs: list, tenant_id: str) -> int:
    """Insert one ``applicant_identity_xref`` row per (applicant_id,
    source_channel, source_document_id) tuple seen in ``docs``. UPSERT
    DO NOTHING; safe to re-run. Returns the number of rows ATTEMPTED
    (PG-side DO NOTHING means some may have been existing already)."""
    written = 0
    for d in docs:
        aid = d.get("applicant_id")
        sys = d.get("source_channel")
        sid = d.get("source_document_id")
        if not aid or not sys or not sid:
            continue
        try:
            await pg.save_xref({
                "applicant_id":     aid,
                "source_system":    sys,
                "source_id":        sid,
                "match_confidence": 1.0,
                "match_method":     "deterministic",
            })
            written += 1
        except Exception as exc:
            logger.warning(
                "xref_save_failed",
                extra={
                    "applicant_id": aid, "source_system": sys,
                    "source_id":    sid, "error":        str(exc)[:200],
                },
            )
    return written


async def _compose_and_upsert_entity_state(
    pg, redis,
    application_id: str,
    applicant_id:   str,
    co_applicant_id: Optional[str],
    los_id:         str,
    loan_data:      dict,
    primary_credit: Optional[dict],
    co_credit:      Optional[dict],
    profile,
    primary_docs:   list,
    co_docs:        list,
    tenant_id:      str,
) -> dict:
    """Compose the ``entity_states`` row from the just-assembled
    profiles and the existing docs, then UPSERT it. Returns a small
    dict with the indexed-column values for the caller's stats."""
    from core.aggregation.entity_state_builder import (
        build_borrower_state, build_property_state, build_loan_terms_state,
    )

    # ── Borrower + co-borrower JSONB ──────────────────────────────────
    borrower_state = await build_borrower_state(
        pg, redis, applicant_id, application_id, tenant_id,
    )
    co_borrowers_list: list = []
    if co_applicant_id:
        try:
            cb_state = await build_borrower_state(
                pg, redis, co_applicant_id, application_id, tenant_id,
            )
            cb_state["role"] = "co_borrower"
            co_borrowers_list.append(cb_state)
        except Exception as exc:
            logger.warning(
                "co_borrower_state_build_failed",
                extra={"co_applicant_id": co_applicant_id, "error": str(exc)[:200]},
            )

    # ── Property + loan_terms JSONB ──────────────────────────────────
    try:
        _prop_id, property_state = await build_property_state(
            pg, redis, application_id, tenant_id,
        )
    except Exception as exc:
        logger.warning(
            "property_state_build_failed",
            extra={"application_id": application_id, "error": str(exc)[:200]},
        )
        property_state = {}

    try:
        loan_terms_state = await build_loan_terms_state(
            pg, application_id, tenant_id,
        )
    except Exception as exc:
        logger.warning(
            "loan_terms_state_build_failed",
            extra={"application_id": application_id, "error": str(exc)[:200]},
        )
        loan_terms_state = {}

    # ── Doc set + verifications (12 flags) ───────────────────────────
    doc_types: set = set()
    total_liquid = 0.0
    for d in primary_docs + co_docs:
        dt = d.get("document_type")
        if dt:
            doc_types.add(dt)
        ef = d.get("extracted_fields") or {}
        if isinstance(ef, dict):
            eb = _f(ef.get("ending_balance"))
            if eb:
                total_liquid += eb

    verifications = {
        "income_verified":     ("W2_CURRENT" in doc_types) or ("PAYSTUB_CURRENT" in doc_types),
        "employment_verified": "VOE_TWN" in doc_types,
        "credit_pulled":       "CREDIT_REPORT" in doc_types,
        "assets_verified":     any(t.startswith("BANK_STATEMENT") for t in doc_types),
        "identity_complete":   ("DRIVERS_LICENSE" in doc_types or "IDENTITY_DL" in doc_types)
                                and ("SSN_VALIDATION" in doc_types),
        "appraisal_complete":  any(t.startswith("APPRAISAL") for t in doc_types),
        "title_clear":         ("TITLE_COMMITMENT" in doc_types)
                                and ("TITLE_INSURANCE" in doc_types),
        "insurance_bound":     ("HOI_BINDER" in doc_types) or ("HOI_DECLARATIONS" in doc_types),
        "aus_approved":        bool((loan_terms_state.get("aus_findings") or {}).get("approved")),
        "rate_locked":         "RATE_LOCK" in doc_types,
        "conditions_cleared":  False,
        "clear_to_close":      False,
    }
    completeness_pct = sum(1 for v in verifications.values() if v) / len(_VERIFICATIONS) * 100.0
    status           = _classify_status(completeness_pct)

    # ── Indexed columns ──────────────────────────────────────────────
    p_score = (primary_credit or {}).get("mid_score")
    c_score = (co_credit or {}).get("mid_score") if co_credit else None
    scores = [s for s in (p_score, c_score) if s is not None]
    mid_score = min(scores) if scores else None

    p_qual = _f((profile.primary_borrower or {}).get("qualifying_monthly")) or 0.0
    c_qual = _f((profile.co_borrower or {}).get("qualifying_monthly")) if profile.co_borrower else None
    combined = _f(profile.combined_qualifying_monthly) or (p_qual + (c_qual or 0.0))

    urla            = loan_terms_state.get("urla") or {}
    rate_lock       = loan_terms_state.get("rate_lock") or {}
    purchase_agree  = loan_terms_state.get("purchase_agreement") or {}
    valuation       = property_state.get("valuation") or {}
    tax_block       = property_state.get("tax") or {}
    insurance_block = property_state.get("insurance") or {}

    loan_amount    = _f(loan_data.get("loan_amount")) \
                      or _f(urla.get("loan_amount")) \
                      or _f(rate_lock.get("loan_amount"))
    interest_rate  = _f(loan_data.get("interest_rate")) \
                      or _f(rate_lock.get("locked_rate")) \
                      or _f(urla.get("interest_rate"))
    term_months    = loan_data.get("loan_term_months") or urla.get("loan_term_months") or 360

    appraised      = _f(valuation.get("appraised_value"))
    purchase_price = _f(purchase_agree.get("purchase_price"))

    # LTV — refi: appraised only; purchase: min(appraised, purchase)
    ltv = None
    if loan_amount:
        if purchase_price and appraised:
            denom = min(purchase_price, appraised)
        else:
            denom = appraised or purchase_price
        if denom:
            ltv = float(loan_amount) / float(denom) * 100.0

    mi_pct      = _mi_pct_by_ltv(ltv)
    mi_monthly  = (float(loan_amount or 0) * mi_pct) / 12.0 if loan_amount else 0.0
    annual_tax  = tax_block.get("annual_tax")
    annual_hoi  = insurance_block.get("annual_premium")
    piti_total  = _piti(loan_amount, interest_rate, term_months, annual_tax, annual_hoi, mi_monthly)

    obligations = (_f((primary_credit or {}).get("total_monthly_obligations")) or 0.0) \
                + (_f((co_credit or {}).get("total_monthly_obligations")) or 0.0 if co_credit else 0.0)

    dti_front = (piti_total / combined * 100.0) if (piti_total and combined) else None
    dti_back  = ((piti_total + obligations) / combined * 100.0) if (piti_total and combined) else None

    # ── Edge + conflict counts (best-effort) ─────────────────────────
    edge_count = 0
    conflict_count = 0
    try:
        edge_count = await pg.count_edges_for_entity(applicant_id, tenant_id=tenant_id)
    except Exception:
        pass
    try:
        conflict_count = await pg.count_conflicts_for_entity(applicant_id, tenant_id=tenant_id)
    except Exception:
        pass

    # ── Compose ``state_data`` mapping to entity_states columns ──────
    state_data = {
        "los_id":                         los_id,
        "legacy_ids":                     {"los_id": los_id} if los_id else {},
        "borrower":                       borrower_state or {},
        "co_borrowers":                   co_borrowers_list,
        "property":                       property_state or {},
        "loan_terms":                     loan_terms_state or {},
        "verifications":                  verifications,
        "mid_credit_score":               mid_score,
        "qualifying_monthly":             p_qual,
        "co_borrower_qualifying_monthly": c_qual,
        "combined_monthly_income":        combined,
        "total_liquid_assets":            total_liquid,
        "appraised_value":                appraised,
        "purchase_price":                 purchase_price,
        "loan_amount":                    loan_amount,
        "interest_rate":                  interest_rate,
        "ltv":                            ltv,
        "cltv":                           ltv,  # no second-mortgage tracking yet
        "dti_front":                      dti_front,
        "dti_back":                       dti_back,
        "piti_monthly":                   piti_total,
        "mi_monthly":                     mi_monthly or None,
        "monthly_obligations":            obligations,
        "existing_mortgage_payment":      None,
        "document_count":                 len(primary_docs) + len(co_docs),
        "graph_edge_count":               edge_count,
        "conflict_count":                 conflict_count,
        "critical_conflict_count":        0,
        "completeness_pct":               completeness_pct,
        "status":                         status,
        "last_decision_by":               None,
        "last_decision_at":               None,
        "decision_trail":                 [],
        # Top-level boolean columns mirror the JSONB verifications block —
        # ``upsert_entity_state`` reads both, and the indexed columns are
        # what reports/dashboards filter on.
        **verifications,
    }

    await pg.upsert_entity_state(application_id, state_data, tenant_id=tenant_id)

    return {
        "completeness_pct": completeness_pct,
        "status":           status,
        "mid_credit_score": mid_score,
        "loan_amount":      loan_amount,
        "doc_count":        len(primary_docs) + len(co_docs),
    }


async def rebuild_one(
    pg, redis,
    application_id: str,
    tenant_id:      str = "default",
    *,
    income_assembler = None,
    credit_assembler = None,
) -> dict:
    """Rebuild every golden-record table for one application from
    already-indexed ``document_index`` + ``document_relationships``
    data. Idempotent — re-running re-UPSERTs.

    Each write is individually idempotent (UPSERT or DELETE+INSERT), so
    a crash mid-application is safe: restart re-runs the application
    and re-writes all five tables. The backfill watermark is only
    advanced AFTER all writes succeed — see ``run_backfill``.

    Returns a stats dict the orchestrator uses for progress logging:
    ``{applicant_count, income_profiles, credit_profiles, xref_rows,
      entity_state, completeness_pct, status, doc_count}``.
    """
    # Lazy-import to avoid a cycle with `service.py` and to let unit
    # tests inject stubs via the optional kwargs above.
    if income_assembler is None:
        from core.income.assembler import IncomeAssembler
        income_assembler = IncomeAssembler()
    if credit_assembler is None:
        from core.credit.assembler import CreditAssembler
        credit_assembler = CreditAssembler()

    app = await pg.get_application(application_id, tenant_id=tenant_id)
    if not app:
        return {"skipped": True, "reason": "application_not_found"}

    applicant_id    = app.get("applicant_id")
    co_applicant_id = app.get("co_applicant_id")
    los_id          = app.get("los_id") or ""
    loan_data       = {
        "loan_amount":       app.get("loan_amount"),
        "interest_rate":     app.get("interest_rate"),
        "loan_term_months":  app.get("loan_term_months"),
    }
    if not applicant_id:
        return {"skipped": True, "reason": "no_applicant_id"}

    primary_docs = await pg.get_documents_for_applicant(
        applicant_id, tenant_id=tenant_id,
    )
    co_docs = (
        await pg.get_documents_for_applicant(co_applicant_id, tenant_id=tenant_id)
        if co_applicant_id else []
    )

    # ── Credit assemblers first (income reads them) ──────────────────
    primary_credit = await credit_assembler.assemble(
        applicant_id, loan_data, postgres_store=pg,
    )
    co_credit = None
    if co_applicant_id:
        co_credit = await credit_assembler.assemble(
            co_applicant_id, loan_data, postgres_store=pg,
        )

    # ── Income assembler ─────────────────────────────────────────────
    profile = income_assembler.assemble(
        primary_docs=primary_docs,
        co_borrower_docs=co_docs,
        primary_credit=primary_credit,
        co_borrower_credit=co_credit,
        application_id=application_id,
        applicant_id=applicant_id,
        co_applicant_id=co_applicant_id,
    )

    # ── Persist profiles (DELETE+INSERT — idempotent for re-runs) ───
    await pg.save_income_profile(profile.model_dump(), tenant_id=tenant_id)
    await pg.save_credit_profile(primary_credit, tenant_id=tenant_id)
    if co_credit:
        await pg.save_credit_profile(co_credit, tenant_id=tenant_id)

    # Mirror to Redis so the next /application/{id}/context read isn't
    # a cold cache. Failures are non-fatal — PG is the source of truth.
    try:
        await redis.set_income_profile(
            applicant_id, profile.model_dump(), tenant_id=tenant_id,
        )
        await redis.set_credit_profile(
            applicant_id, primary_credit, tenant_id=tenant_id,
        )
        if co_credit and co_applicant_id:
            await redis.set_credit_profile(
                co_applicant_id, co_credit, tenant_id=tenant_id,
            )
    except Exception as exc:
        logger.warning(
            "redis_profile_cache_failed",
            extra={"applicant_id": applicant_id, "error": str(exc)[:200]},
        )

    # ── Identity xrefs (UPSERT DO NOTHING) ──────────────────────────
    xref_rows = await _save_xrefs_for_docs(
        pg, primary_docs + co_docs, tenant_id,
    )

    # ── entity_states UPSERT + change-log event ─────────────────────
    es = await _compose_and_upsert_entity_state(
        pg, redis, application_id, applicant_id, co_applicant_id,
        los_id, loan_data, primary_credit, co_credit, profile,
        primary_docs, co_docs, tenant_id,
    )

    try:
        await pg.log_entity_state_event(
            application_id=application_id,
            event_type="golden_record_rebuilt",
            triggered_by="rebuild_one",
            tenant_id=tenant_id,
        )
    except Exception as exc:
        logger.warning(
            "entity_state_event_log_failed",
            extra={"application_id": application_id, "error": str(exc)[:200]},
        )

    return {
        "applicant_count":  1 + (1 if co_applicant_id else 0),
        "income_profiles":  1 + (1 if profile.co_borrower else 0),
        "credit_profiles":  1 + (1 if co_credit else 0),
        "xref_rows":        xref_rows,
        "entity_state":     True,
        "completeness_pct": es["completeness_pct"],
        "status":           es["status"],
        "doc_count":        es["doc_count"],
    }


# ----- Backfill orchestrator -------------------------------------------------

async def run_backfill(
    pg, redis,
    *,
    tenant_id:     str  = "default",
    force:         bool = False,
    batch_size:    int  = 50,
    max_errors:    int  = 100,
) -> dict:
    """Drain every application through ``rebuild_one``. Restartable on
    crash: progress is committed to ``golden_record_backfill_state``
    after every application, and the next call resumes after
    ``last_completed_application_id``.

    Args:
        force:        Reset the watermark before starting. Re-UPSERTs
                      every application from scratch.
        batch_size:   How many applications to log progress against
                      (the orchestrator commits per application — this
                      only affects the "memory release" + progress log
                      cadence).
        max_errors:   Stop early if this many applications fail in a
                      row. Default 100 — most likely a deeper bug
                      than a single bad app, worth bailing for ops to
                      inspect.

    Returns a final-state dict the operator sees on completion.
    """
    state = await read_backfill_state(tenant_id=tenant_id)
    if force or state.get("status") in ("completed", "failed", "not_started"):
        state = {
            "tenant_id":                     tenant_id,
            "last_completed_application_id": None,
            "completed_count":               0,
            "total_count":                   await count_applications(tenant_id),
            "status":                        "running",
            "errors":                        [],
            "started_at":                    datetime.now(timezone.utc).isoformat(),
            "completed_at":                  None,
        }
    else:
        # Resume from prior run — keep the existing cursor + errors.
        state["status"]      = "running"
        state["total_count"] = await count_applications(tenant_id)
        state.setdefault("errors", [])

    await write_backfill_state(state, tenant_id=tenant_id)

    processed = 0
    after = state.get("last_completed_application_id")
    async for application_id in iter_application_ids_for_backfill(
        tenant_id, after_application_id=after,
    ):
        try:
            await rebuild_one(
                pg, redis, application_id, tenant_id=tenant_id,
            )
            state["last_completed_application_id"] = application_id
            state["completed_count"]               = int(state.get("completed_count") or 0) + 1
            await write_backfill_state(state, tenant_id=tenant_id)
        except Exception as exc:
            logger.error(
                "golden_record_rebuild_failed",
                extra={"application_id": application_id, "error": str(exc)[:500]},
            )
            state.setdefault("errors", []).append({
                "application_id": application_id,
                "error":          f"{type(exc).__name__}: {str(exc)[:400]}",
                "at":             datetime.now(timezone.utc).isoformat(),
            })
            # Cap the error list so a runaway loop doesn't bloat the row.
            state["errors"] = state["errors"][-max_errors:]
            await write_backfill_state(state, tenant_id=tenant_id)
            if len([e for e in state["errors"]
                    if e.get("application_id") == application_id]) >= max_errors:
                state["status"]       = "failed"
                state["completed_at"] = datetime.now(timezone.utc).isoformat()
                await write_backfill_state(state, tenant_id=tenant_id)
                return state

        processed += 1
        if processed % batch_size == 0:
            logger.info(
                f"golden_record_backfill_progress "
                f"completed={state['completed_count']} "
                f"total={state['total_count']} "
                f"last={state['last_completed_application_id']}"
            )

    state["status"]       = "completed"
    state["completed_at"] = datetime.now(timezone.utc).isoformat()
    await write_backfill_state(state, tenant_id=tenant_id)
    logger.info(
        f"golden_record_backfill_complete "
        f"completed={state['completed_count']}/{state['total_count']}"
    )
    return state
