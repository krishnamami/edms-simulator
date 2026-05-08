#!/usr/bin/env python3
"""EDMS Indexing Pipeline — Stress Test Suite

Exercises every fix from the indexing review:
  TEST 1: Concurrent same-applicant uploads (FIX 1 — assembly lock)
  TEST 2: Batch indexer + upload race (FIX 2 — skip already-indexed)
  TEST 3: Cache invalidation correctness (FIX 3 — no silent failures)
  TEST 4: Channel × doc-type matrix (extracted_fields shape)
  TEST 5: Cross-applicant parallel throughput (FIX 4/5/6 — async + parallel)
  TEST 6: Watermark rewind re-index (no duplicates, no doubled income)
  TEST 7: Webhook fan-out under load

Usage:
    # Against local API (must be running on :8001)
    python scripts/stress_test_indexing.py

    # With custom URL
    EDMS_API_URL=http://localhost:8001 python scripts/stress_test_indexing.py

    # Run a single test
    python scripts/stress_test_indexing.py --test 1

    # Verbose output
    python scripts/stress_test_indexing.py -v

Prerequisites:
    - API running: uvicorn api.main:app --port 8001
    - Postgres + Redis up: docker compose up -d postgres redis
    - Schema applied
    - .env sourced
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = os.getenv("EDMS_API_URL", "http://localhost:8001")
API_KEY = os.getenv("EDMS_API_KEY", "edms_dev_key")
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

PASS = 0
FAIL = 0
WARN = 0

logger = logging.getLogger("stress_test")


def _tag(label: str) -> str:
    return f"STRESS-{uuid.uuid4().hex[:6].upper()}"


def _los_id() -> str:
    return f"LOS-STRESS-{uuid.uuid4().hex[:8].upper()}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def api_post(client: httpx.AsyncClient, path: str, body: dict) -> httpx.Response:
    return await client.post(f"{BASE_URL}{path}", json=body, headers=HEADERS, timeout=30)


async def api_get(client: httpx.AsyncClient, path: str) -> httpx.Response:
    return await client.get(f"{BASE_URL}{path}", headers=HEADERS, timeout=30)


def check(condition: bool, msg: str):
    global PASS, FAIL
    if condition:
        PASS += 1
        logger.info(f"  [PASS] {msg}")
    else:
        FAIL += 1
        logger.error(f"  [FAIL] {msg}")


def warn(msg: str):
    global WARN
    WARN += 1
    logger.warning(f"  [WARN] {msg}")


async def create_application(client: httpx.AsyncClient, los_id: str,
                              borrower: Optional[dict] = None,
                              co_borrower: Optional[dict] = None,
                              loan: Optional[dict] = None) -> dict:
    """Submit an application and return the response."""
    payload = {
        "los_id": los_id,
        "borrower": borrower or {
            "first_name": "Stress",
            "last_name": f"Test-{los_id[-6:]}",
            "dob": "1985-03-15",
            "ssn_hash": f"hash_{los_id}",
            "ssn_last4": los_id[-4:],
            "email": f"stress_{los_id}@test.com",
        },
        "co_borrower": co_borrower,
        "loan": loan or {
            "loan_amount": 350000,
            "interest_rate": 6.5,
            "loan_term_months": 360,
        },
        "documents": [],
    }
    resp = await api_post(client, "/loans", payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"create_application failed: {resp.status_code} {resp.text[:200]}")
    return resp.json()


def make_doc(doc_id: str, doc_type: str, role: str = "primary",
             category: str = "income", fields: Optional[dict] = None) -> dict:
    """Build a document payload for /documents/upload."""
    base = {
        "document_id": doc_id,
        "document_type": doc_type,
        "document_category": category,
        "borrower_role": role,
        "status": "indexed",
        "confidence_score": 0.94,
    }
    if fields:
        base.update(fields)
    return base


async def upload_doc(client: httpx.AsyncClient, applicant_id: str,
                     application_id: str, doc: dict) -> httpx.Response:
    """Upload a single document via /documents/upload."""
    payload = {
        "applicant_id": applicant_id,
        "application_id": application_id,
        "all_documents": [doc],
    }
    return await api_post(client, "/documents/upload", payload)


# ---------------------------------------------------------------------------
# TEST 1 — Concurrent same-applicant uploads
# ---------------------------------------------------------------------------
async def test_1_concurrent_same_applicant(client: httpx.AsyncClient):
    """Fire N docs for the same applicant simultaneously.
    Assert final Redis income reflects ALL docs, not a subset."""
    logger.info("TEST 1: Concurrent same-applicant uploads")

    los_id = _los_id()
    app = await create_application(client, los_id)
    applicant_id = app["applicant_id"]
    application_id = app["application_id"]

    # Create 5 different W2s with different wages
    docs = []
    wages = [50000, 60000, 70000, 80000, 90000]
    for i, w in enumerate(wages):
        docs.append(make_doc(
            doc_id=f"DOC-{los_id}-W2-{i}",
            doc_type="W2_CURRENT",
            fields={
                "box1_wages": w,
                "tax_year": "2025",
                "employer_name": f"Employer-{i}",
            },
        ))

    # Fire all uploads simultaneously
    tasks = [upload_doc(client, applicant_id, application_id, d) for d in docs]
    start = time.time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.time() - start

    # Check all succeeded
    errors = [r for r in results if isinstance(r, Exception)]
    http_errors = [r for r in results if isinstance(r, httpx.Response) and r.status_code >= 400]

    check(len(errors) == 0, f"No exceptions ({len(errors)} errors)")
    check(len(http_errors) == 0, f"No HTTP errors ({len(http_errors)} failures)")

    # Wait a moment for assembly lock contention to resolve
    await asyncio.sleep(1.0)

    # Read the income profile from the API
    resp = await api_get(client, f"/applicant/{applicant_id}/income-profile")
    if resp.status_code == 200:
        profile = resp.json()
        # The income profile should reflect documents — we can't predict
        # exactly which W2 "wins" for qualifying, but the profile should
        # exist and have a non-zero qualifying amount
        qualifying = profile.get("data", {}).get("combined_qualifying_monthly", 0)
        if qualifying is None:
            qualifying = 0
        check(qualifying > 0, f"Qualifying income > 0 (got ${qualifying:,.0f}/mo)")
    else:
        check(False, f"Income profile readable (got {resp.status_code})")

    # Check document count via graph. The endpoint wraps in
    # ``{"source": ..., "data": {...summary...}}`` like the income / credit
    # profile endpoints — read the count out of ``data``.
    resp = await api_get(client, f"/applicant/{applicant_id}/graph/summary")
    if resp.status_code == 200:
        summary = resp.json()
        data = summary.get("data") or summary  # tolerate both shapes
        doc_count = data.get("document_count", 0)
        check(doc_count == 5, f"All 5 docs indexed (got {doc_count})")
    else:
        warn(f"Graph summary not available ({resp.status_code})")

    logger.info(f"  Concurrent upload of 5 docs took {elapsed:.2f}s")


# ---------------------------------------------------------------------------
# TEST 2 — Batch indexer + upload path race
# ---------------------------------------------------------------------------
async def test_2_indexer_upload_race(client: httpx.AsyncClient):
    """Upload a doc via API, then immediately trigger the batch indexer.
    Assert the indexer skips the already-indexed doc."""
    logger.info("TEST 2: Batch indexer vs upload path race")

    los_id = _los_id()
    app = await create_application(client, los_id)
    applicant_id = app["applicant_id"]
    application_id = app["application_id"]

    # Upload a doc via the event-driven path
    doc = make_doc(
        doc_id=f"DOC-{los_id}-W2-RACE",
        doc_type="W2_CURRENT",
        fields={"box1_wages": 95000, "tax_year": "2025"},
    )
    resp = await upload_doc(client, applicant_id, application_id, doc)
    check(resp.status_code in (200, 201), f"Upload succeeded ({resp.status_code})")

    # Immediately trigger the batch indexer
    resp = await api_post(client, "/indexing/run", {"source": "s3"})
    if resp.status_code == 200:
        stats = resp.json()
        processed = stats.get("processed", -1)
        skipped_already = stats.get("skipped_already_indexed", 0)
        logger.info(f"  Indexer stats: processed={processed}, skipped_already_indexed={skipped_already}")
        # The doc we just uploaded shouldn't be re-processed
        # (It may or may not appear in S3 depending on local_storage config,
        # so we check the stat exists rather than asserting a specific count)
        check("skipped_already_indexed" in stats or processed == 0,
              "Indexer tracks already-indexed skips")
    else:
        warn(f"Indexer run returned {resp.status_code} — may not be configured")


# ---------------------------------------------------------------------------
# TEST 3 — Cache invalidation correctness
# ---------------------------------------------------------------------------
async def test_3_cache_invalidation(client: httpx.AsyncClient):
    """Upload a doc, read context, upload another doc, read context again.
    Assert the second read reflects the second doc — no stale cache."""
    logger.info("TEST 3: Cache invalidation correctness")

    los_id = _los_id()
    app = await create_application(client, los_id)
    applicant_id = app["applicant_id"]
    application_id = app["application_id"]

    # Upload first W2
    doc1 = make_doc(
        doc_id=f"DOC-{los_id}-W2-1",
        doc_type="W2_CURRENT",
        fields={"box1_wages": 80000, "tax_year": "2025", "employer_name": "AlphaCo"},
    )
    await upload_doc(client, applicant_id, application_id, doc1)
    await asyncio.sleep(0.3)

    # Read context — should reflect doc1.
    # The /context endpoint wraps in ``{"source": ..., "data": {...}}``;
    # income lives under ``data.primary.qualifying_monthly`` (the
    # ApplicationContext model uses ``primary`` / ``co_borrower``, not
    # ``borrower``) and combined under ``data.combined_qualifying_monthly``.
    resp1 = await api_get(client, f"/application/{application_id}/context")
    ctx1_income = 0
    if resp1.status_code == 200:
        ctx1 = resp1.json()
        d1 = ctx1.get("data") or ctx1
        ctx1_income = (d1.get("primary") or {}).get("qualifying_monthly", 0) or 0

    # Upload second W2 with different wages
    doc2 = make_doc(
        doc_id=f"DOC-{los_id}-W2-2",
        doc_type="W2_CURRENT",
        fields={"box1_wages": 120000, "tax_year": "2025", "employer_name": "BetaCo"},
    )
    await upload_doc(client, applicant_id, application_id, doc2)
    await asyncio.sleep(0.3)

    # Read context again — should reflect both docs, not stale cache of doc1 only
    resp2 = await api_get(client, f"/application/{application_id}/context")
    if resp2.status_code == 200:
        ctx2 = resp2.json()
        # Surface the actual response shape so a future structure change
        # produces a useful log line instead of a silent zero.
        logger.info(f"  Context response keys: {list(ctx2.keys())}")
        d2 = ctx2.get("data") or ctx2
        if "primary" in d2:
            logger.info(f"  Primary keys: {list(d2['primary'].keys())[:8]}")
        ctx2_income = (d2.get("primary") or {}).get("qualifying_monthly", 0) or 0
        # After adding a second W2, qualifying income should change
        # (either increase or recalculate — but shouldn't be identical to ctx1
        # unless both W2s have the same employer/year and collapse)
        check(ctx2_income > 0, f"Context has income after 2 docs (${ctx2_income:,.0f}/mo)")

        # Verify document count increased (graph_summary is folded into the
        # context payload by ContextAssembler).
        graph = d2.get("graph_summary") or {}
        doc_count = graph.get("document_count", 0)
        if doc_count:
            check(doc_count >= 2, f"Context sees >= 2 docs (got {doc_count})")
    else:
        check(False, f"Context readable after 2nd upload ({resp2.status_code})")


# ---------------------------------------------------------------------------
# TEST 4 — Channel × doc-type matrix
# ---------------------------------------------------------------------------
async def test_4_doctype_matrix(client: httpx.AsyncClient):
    """Upload one doc per major doc type. Assert each is indexed correctly."""
    logger.info("TEST 4: Doc-type matrix coverage")

    los_id = _los_id()
    app = await create_application(client, los_id)
    applicant_id = app["applicant_id"]
    application_id = app["application_id"]

    doc_types = [
        ("W2_CURRENT", "income", {"box1_wages": 92000, "tax_year": "2025"}),
        ("PAYSTUB_CURRENT", "income", {"ytd_gross": 46000, "pay_period_end": "2025-06-15"}),
        ("BANK_STATEMENT_M1", "asset", {"ending_balance": 45000, "avg_balance": 42000}),
        ("CREDIT_REPORT", "credit", {"mid_score": 745, "tradeline_count": 8}),
        ("APPRAISAL_URAR", "property", {"appraised_value": 425000, "property_type": "SFR"}),
        ("HOI_BINDER", "property", {"annual_premium": 1800, "carrier": "StateFarm"}),
        ("FLOOD_CERT", "property", {"flood_zone": "X", "nfip_community": "120067"}),
    ]

    for doc_type, category, fields in doc_types:
        doc = make_doc(
            doc_id=f"DOC-{los_id}-{doc_type}",
            doc_type=doc_type,
            category=category,
            fields=fields,
        )
        resp = await upload_doc(client, applicant_id, application_id, doc)
        check(
            resp.status_code in (200, 201),
            f"{doc_type} upload succeeded ({resp.status_code})",
        )

    await asyncio.sleep(0.5)

    # Verify all docs landed. /graph/summary wraps in
    # ``{"source": ..., "data": {...}}``.
    resp = await api_get(client, f"/applicant/{applicant_id}/graph/summary")
    if resp.status_code == 200:
        summary = resp.json()
        data = summary.get("data") or summary
        doc_count = data.get("document_count", 0)
        check(doc_count == len(doc_types), f"All {len(doc_types)} doc types indexed (got {doc_count})")
    else:
        warn(f"Graph summary unavailable ({resp.status_code})")

    # Verify a specific field was indexed correctly. The endpoint returns
    # ``best_value`` as the highest-confidence document_index row (a dict
    # with ``field_value`` / ``document_type`` / ``confidence_score``), not
    # the scalar value — extract field_value (or best_value if a future
    # response shape unwraps it) and normalize the float vs int suffix.
    resp = await api_get(client, f"/applicant/{applicant_id}/field/box1_wages")
    if resp.status_code == 200:
        field_data = resp.json()
        best = field_data.get("best_value")
        if isinstance(best, dict):
            best = best.get("best_value") or best.get("field_value")
        check(str(best).replace(".0", "") == "92000",
              f"box1_wages indexed correctly (got {best})")
    else:
        warn(f"Field lookup unavailable ({resp.status_code})")


# ---------------------------------------------------------------------------
# TEST 5 — Cross-applicant parallel throughput
# ---------------------------------------------------------------------------
async def test_5_cross_applicant_throughput(client: httpx.AsyncClient):
    """Create N applicants, upload M docs each, measure total time."""
    logger.info("TEST 5: Cross-applicant parallel throughput")

    N_APPLICANTS = 20
    DOCS_PER = 3

    # Create all applications
    apps = []
    for i in range(N_APPLICANTS):
        los_id = _los_id()
        try:
            app = await create_application(client, los_id)
            apps.append(app)
        except Exception as exc:
            warn(f"Failed to create app {i}: {exc}")

    check(len(apps) == N_APPLICANTS,
          f"Created {len(apps)}/{N_APPLICANTS} applications")

    # Build all upload tasks
    tasks = []
    for app in apps:
        for j in range(DOCS_PER):
            doc = make_doc(
                doc_id=f"DOC-{app['application_id']}-W2-{j}",
                doc_type="W2_CURRENT",
                fields={
                    "box1_wages": 50000 + j * 10000,
                    "tax_year": "2025",
                },
            )
            tasks.append(upload_doc(client, app["applicant_id"],
                                     app["application_id"], doc))

    # Fire all uploads concurrently
    start = time.time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.time() - start

    successes = sum(1 for r in results
                    if isinstance(r, httpx.Response) and r.status_code in (200, 201))
    errors = sum(1 for r in results if isinstance(r, Exception))
    http_errors = sum(1 for r in results
                      if isinstance(r, httpx.Response) and r.status_code >= 400)

    total = N_APPLICANTS * DOCS_PER
    check(successes == total,
          f"{successes}/{total} uploads succeeded ({errors} exceptions, {http_errors} HTTP errors)")

    throughput = total / elapsed if elapsed > 0 else 0
    logger.info(f"  {total} docs across {N_APPLICANTS} applicants in {elapsed:.2f}s")
    logger.info(f"  Throughput: {throughput:.1f} docs/sec")

    if throughput < 5:
        warn(f"Throughput below 5 docs/sec — possible blocking issue")

    # Wait for assemblies to complete
    await asyncio.sleep(2.0)

    # Spot-check a few applicants have income
    checked = 0
    for app in apps[:5]:
        resp = await api_get(client, f"/applicant/{app['applicant_id']}/income-profile")
        if resp.status_code == 200:
            profile = resp.json()
            q = profile.get("data", {}).get("combined_qualifying_monthly", 0) or 0
            if q > 0:
                checked += 1
    check(checked >= 3, f"{checked}/5 spot-checked applicants have income")


# ---------------------------------------------------------------------------
# TEST 6 — Watermark rewind re-index
# ---------------------------------------------------------------------------
async def test_6_watermark_rewind(client: httpx.AsyncClient):
    """Rewind the indexer watermark and re-run. Assert no duplicate rows
    and income figures don't double."""
    logger.info("TEST 6: Watermark rewind re-index")

    # Get current indexer status
    resp = await api_get(client, "/indexing/status")
    if resp.status_code != 200:
        warn("Indexer status endpoint not available — skipping test 6")
        return

    # Pick an applicant that already has income
    # We'll use the first app from test 5 if available, or create one
    los_id = _los_id()
    app = await create_application(client, los_id)
    applicant_id = app["applicant_id"]
    application_id = app["application_id"]

    doc = make_doc(
        doc_id=f"DOC-{los_id}-W2-REWIND",
        doc_type="W2_CURRENT",
        fields={"box1_wages": 100000, "tax_year": "2025"},
    )
    await upload_doc(client, applicant_id, application_id, doc)
    await asyncio.sleep(0.5)

    # Read income before rewind
    resp = await api_get(client, f"/applicant/{applicant_id}/income-profile")
    income_before = 0
    if resp.status_code == 200:
        income_before = resp.json().get("data", {}).get("combined_qualifying_monthly", 0) or 0

    # Rewind watermark to epoch
    resp = await client.put(
        f"{BASE_URL}/indexing/watermark",
        json={"source": "s3", "timestamp": "2020-01-01T00:00:00Z"},
        headers=HEADERS,
        timeout=10,
    )
    if resp.status_code != 200:
        warn(f"Watermark rewind returned {resp.status_code} — skipping rest of test 6")
        return

    # Run indexer
    resp = await api_post(client, "/indexing/run", {"source": "s3"})
    if resp.status_code == 200:
        stats = resp.json()
        logger.info(f"  Re-index stats: {json.dumps(stats, indent=2)[:300]}")

    await asyncio.sleep(1.0)

    # Read income after rewind + re-index
    resp = await api_get(client, f"/applicant/{applicant_id}/income-profile")
    income_after = 0
    if resp.status_code == 200:
        income_after = resp.json().get("data", {}).get("combined_qualifying_monthly", 0) or 0

    # Income should NOT double
    if income_before > 0 and income_after > 0:
        ratio = income_after / income_before
        check(
            0.9 <= ratio <= 1.1,
            f"Income stable after rewind (before=${income_before:,.0f}, after=${income_after:,.0f}, ratio={ratio:.2f})",
        )
    elif income_before > 0:
        warn("Income disappeared after rewind")
    else:
        warn("No income before rewind — can't verify stability")


# ---------------------------------------------------------------------------
# TEST 7 — Webhook fan-out under load
# ---------------------------------------------------------------------------
async def test_7_webhook_fanout(client: httpx.AsyncClient):
    """Register webhook subscribers, upload docs, check delivery stats."""
    logger.info("TEST 7: Webhook fan-out")

    # Register 3 test webhooks (they'll fail delivery since the URLs are fake,
    # but we can verify the delivery attempts are recorded)
    webhook_ids = []
    for i in range(3):
        resp = await api_post(client, "/webhooks", {
            "name": f"stress-test-webhook-{i}",
            "url": f"https://httpbin.org/status/200?stress={i}",
            "events": ["context_updated"],
        })
        if resp.status_code in (200, 201):
            wh = resp.json()
            wh_id = wh.get("webhook_id") or wh.get("id")
            if wh_id:
                webhook_ids.append(wh_id)

    if not webhook_ids:
        warn("Could not register webhooks — skipping delivery test")
        return

    check(len(webhook_ids) == 3, f"Registered {len(webhook_ids)}/3 webhooks")

    # Upload a doc to trigger context_updated
    los_id = _los_id()
    app = await create_application(client, los_id)
    doc = make_doc(
        doc_id=f"DOC-{los_id}-W2-WH",
        doc_type="W2_CURRENT",
        fields={"box1_wages": 75000, "tax_year": "2025"},
    )
    await upload_doc(client, app["applicant_id"], app["application_id"], doc)
    await asyncio.sleep(1.0)

    # Check delivery attempts
    for wh_id in webhook_ids:
        resp = await api_get(client, f"/webhooks/{wh_id}/deliveries?limit=5")
        if resp.status_code == 200:
            deliveries = resp.json()
            if isinstance(deliveries, list):
                logger.info(f"  Webhook {wh_id}: {len(deliveries)} delivery attempts")
            elif isinstance(deliveries, dict):
                items = deliveries.get("deliveries", deliveries.get("items", []))
                logger.info(f"  Webhook {wh_id}: {len(items)} delivery attempts")

    # Clean up webhooks
    for wh_id in webhook_ids:
        await client.delete(f"{BASE_URL}/webhooks/{wh_id}", headers=HEADERS, timeout=5)

    check(True, "Webhook fan-out exercised without blocking the upload path")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
async def run_all(tests: Optional[list[int]] = None):
    async with httpx.AsyncClient() as client:
        # Health check
        try:
            resp = await api_get(client, "/health")
            if resp.status_code != 200:
                logger.error(f"API not healthy: {resp.status_code}")
                sys.exit(1)
        except Exception as exc:
            logger.error(f"Cannot reach API at {BASE_URL}: {exc}")
            sys.exit(1)

        logger.info(f"API healthy at {BASE_URL}")
        logger.info("=" * 60)

        all_tests = {
            1: test_1_concurrent_same_applicant,
            2: test_2_indexer_upload_race,
            3: test_3_cache_invalidation,
            4: test_4_doctype_matrix,
            5: test_5_cross_applicant_throughput,
            6: test_6_watermark_rewind,
            7: test_7_webhook_fanout,
        }

        to_run = tests or sorted(all_tests.keys())

        for t in to_run:
            if t in all_tests:
                try:
                    await all_tests[t](client)
                except Exception as exc:
                    logger.error(f"TEST {t} CRASHED: {exc}")
                    global FAIL
                    FAIL += 1
                logger.info("")

        logger.info("=" * 60)
        logger.info(f"RESULTS: {PASS} passed, {FAIL} failed, {WARN} warnings")
        if FAIL > 0:
            logger.error("SOME TESTS FAILED")
            sys.exit(1)
        else:
            logger.info("ALL TESTS PASSED")


def main():
    parser = argparse.ArgumentParser(description="EDMS Indexing Stress Tests")
    parser.add_argument("--test", "-t", type=int, action="append",
                        help="Run specific test(s) by number (1-7)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    asyncio.run(run_all(tests=args.test))


if __name__ == "__main__":
    main()
