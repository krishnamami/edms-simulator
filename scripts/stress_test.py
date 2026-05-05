"""Concurrent stress test against the EDMS Simulator API.

Reports throughput + P50/P95/P99 latency per endpoint, error rate, and cache
hit rate. Exits non-zero if error_rate > 1% or P99 > 2000ms.

Example:
    python scripts/stress_test.py --applications 1000 --concurrency 50
"""
from __future__ import annotations

import argparse
import asyncio
import math
import os
import random
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

from core.identity.golden_record import GoldenRecord  # noqa: E402

ERROR_RATE_THRESHOLD = 0.01
P99_LATENCY_THRESHOLD_MS = 2000.0


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return s[int(k)]
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _make_loan(i: int) -> dict:
    ssn = f"{random.randint(100,999):03d}-{random.randint(10,99):02d}-{random.randint(1000,9999):04d}"
    return {
        "los_id": f"LOS-STRESS-{i:08d}",
        "borrower": {
            "first_name": f"User{i}",
            "last_name": f"Stress{i}",
            "dob": f"19{random.randint(60,99):02d}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "ssn_hash": GoldenRecord.hash_ssn(ssn),
            "ssn_last4": ssn[-4:],
            "email": f"u{i}@example.com",
        },
        "loan": {"credit_band": "near-prime"},
        "documents": [
            {
                "document_id": f"D-STRESS-{i:08d}",
                "document_type": "W2",
                "borrower_role": "primary",
                "box1_wages": random.randint(40000, 200000),
                "employer_name": f"E{i}",
            }
        ],
    }


async def _timed(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    start = time.perf_counter()
    try:
        r = await client.request(method, url, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000
        return r, elapsed, None
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return None, elapsed, e


async def stress(applications: int, concurrency: int, api: str):
    headers = {"X-API-Key": os.getenv("API_KEY", "edms_dev_key")}
    sem = asyncio.Semaphore(concurrency)
    latencies: dict[str, list[float]] = defaultdict(list)
    errors: dict[str, int] = defaultdict(int)
    cache_hits = 0
    cache_total = 0
    applicant_ids: list[str] = []

    async with httpx.AsyncClient(base_url=api, timeout=30.0) as client:

        async def submit(i: int):
            async with sem:
                r, ms, err = await _timed(
                    client,
                    "POST",
                    "/loans",
                    headers=headers,
                    json=_make_loan(i),
                )
                latencies["POST /loans"].append(ms)
                if err or r is None or r.status_code != 200:
                    errors["POST /loans"] += 1
                    return None
                return r.json()["applicant_id"]

        print(f"--- POST /loans x {applications} (concurrency={concurrency}) ---")
        t0 = time.perf_counter()
        results = await asyncio.gather(*[submit(i) for i in range(applications)])
        elapsed = time.perf_counter() - t0
        applicant_ids = [a for a in results if a]
        print(
            f"submitted={applications} in {elapsed:.2f}s "
            f"-> {applications/elapsed:,.1f} req/s"
        )

        async def fetch(aid: str):
            nonlocal cache_hits, cache_total
            async with sem:
                r1, ms1, err1 = await _timed(
                    client,
                    "GET",
                    f"/applicant/{aid}/income-profile",
                    headers=headers,
                )
                latencies["GET /income-profile"].append(ms1)
                if err1 or r1 is None or r1.status_code != 200:
                    errors["GET /income-profile"] += 1
                else:
                    body = r1.json()
                    cache_total += 1
                    if body.get("cached"):
                        cache_hits += 1

                r2, ms2, err2 = await _timed(
                    client,
                    "GET",
                    f"/applicant/{aid}/credit-profile",
                    headers=headers,
                )
                latencies["GET /credit-profile"].append(ms2)
                if err2 or r2 is None or r2.status_code != 200:
                    errors["GET /credit-profile"] += 1
                else:
                    body = r2.json()
                    cache_total += 1
                    if body.get("cached"):
                        cache_hits += 1

        sample_size = min(len(applicant_ids), 500)
        sample = random.sample(applicant_ids, sample_size) if applicant_ids else []
        if sample:
            print(f"--- READS x {len(sample)} (each: income + credit) ---")
            await asyncio.gather(*[fetch(a) for a in sample])

    print("\n=== RESULTS ===")
    total_calls = sum(len(v) for v in latencies.values())
    total_errors = sum(errors.values())
    error_rate = (total_errors / total_calls) if total_calls else 0
    cache_hit_rate = (cache_hits / cache_total) if cache_total else 0

    worst_p99 = 0.0
    for endpoint, lats in latencies.items():
        p50 = _percentile(lats, 0.50)
        p95 = _percentile(lats, 0.95)
        p99 = _percentile(lats, 0.99)
        worst_p99 = max(worst_p99, p99)
        err_count = errors.get(endpoint, 0)
        err_pct = (err_count / len(lats) * 100) if lats else 0
        print(
            f"{endpoint:30s} n={len(lats):6d} "
            f"P50={p50:7.1f}ms P95={p95:7.1f}ms P99={p99:7.1f}ms "
            f"errors={err_count} ({err_pct:.2f}%)"
        )
    print(
        f"\nTotal calls: {total_calls}  "
        f"Total errors: {total_errors} ({error_rate*100:.3f}%)"
    )
    print(f"Cache hit rate: {cache_hits}/{cache_total} ({cache_hit_rate*100:.1f}%)")

    fail = False
    if error_rate > ERROR_RATE_THRESHOLD:
        print(f"FAIL: error_rate {error_rate*100:.3f}% > 1%")
        fail = True
    if worst_p99 > P99_LATENCY_THRESHOLD_MS:
        print(f"FAIL: P99 {worst_p99:.1f}ms > {P99_LATENCY_THRESHOLD_MS}ms")
        fail = True
    if fail:
        sys.exit(1)
    print("\nPASS")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--applications", type=int, default=1000)
    ap.add_argument("--concurrency", type=int, default=50)
    ap.add_argument("--api", type=str, default="http://localhost:8001")
    args = ap.parse_args()
    asyncio.run(stress(args.applications, args.concurrency, args.api))


if __name__ == "__main__":
    main()
