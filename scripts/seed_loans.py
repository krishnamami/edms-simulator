"""Seed N synthetic loans against a running EDMS API.

Usage:
    python scripts/seed_loans.py --count 1000 --api http://localhost:8001
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

from core.identity.golden_record import GoldenRecord  # noqa: E402

FIRST_NAMES = ["Alex", "Jordan", "Sam", "Pat", "Casey", "Taylor", "Morgan", "Dana"]
LAST_NAMES = ["Adams", "Brown", "Carter", "Davis", "Evans", "Foster", "Garcia", "Harris"]
BANDS = ["prime", "near-prime", "subprime"]


def _make_loan(i: int) -> dict:
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    ssn = f"{random.randint(100,999):03d}-{random.randint(10,99):02d}-{random.randint(1000,9999):04d}"
    return {
        "los_id": f"LOS-SEED-{i:06d}",
        "borrower": {
            "first_name": fn,
            "last_name": ln,
            "dob": f"19{random.randint(60,99):02d}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "ssn_hash": GoldenRecord.hash_ssn(ssn),
            "ssn_last4": ssn[-4:],
            "email": f"{fn.lower()}.{ln.lower()}@example.com",
        },
        "loan": {"credit_band": random.choice(BANDS)},
        "documents": [
            {
                "document_id": f"D-SEED-{i:06d}-W2",
                "document_type": "W2",
                "borrower_role": "primary",
                "box1_wages": random.randint(40000, 200000),
                "employer_name": f"Employer-{i}",
            },
            {
                "document_id": f"D-SEED-{i:06d}-PAY",
                "document_type": "PAYSTUB",
                "borrower_role": "primary",
            },
        ],
    }


async def seed(count: int, api: str, concurrency: int):
    headers = {"X-API-Key": os.getenv("API_KEY", "edms_dev_key")}
    sem = asyncio.Semaphore(concurrency)
    success = 0
    failure = 0

    async with httpx.AsyncClient(base_url=api, timeout=30.0) as client:
        async def one(i: int):
            nonlocal success, failure
            async with sem:
                try:
                    r = await client.post(
                        "/loans", headers=headers, json=_make_loan(i)
                    )
                    if r.status_code == 200:
                        success += 1
                    else:
                        failure += 1
                except Exception:
                    failure += 1

        await asyncio.gather(*[one(i) for i in range(count)])

    print(f"Seeded {success}/{count} ({failure} failed)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--api", type=str, default="http://localhost:8001")
    ap.add_argument("--concurrency", type=int, default=20)
    args = ap.parse_args()
    asyncio.run(seed(args.count, args.api, args.concurrency))


if __name__ == "__main__":
    main()
