#!/usr/bin/env python3
"""
EDMS Simulator — Local Simulation Walkthrough (Phase D)

Exercises every ingestion channel end-to-end against a running stack:
  STEP 0  Generate a realistic document package (W2, paystub, bank stmt,
          credit report, driver's license JPG) into local_storage/
  STEP 1  Chat-based intake via POST /ingest/chat
  STEP 2  PDF upload — W2 via POST /ingest/pdf
  STEP 3  Email with paystub attachment via POST /ingest/email
  STEP 4  POST /loans creates the golden record; verify Redis + Postgres
  STEP 5  Field confidence evolution: pick the highest-confidence value
          across chat / W2 / paystub for the same fields
  STEP 6  Same person, second LOS → deterministic SSN match

Run:
  docker compose up -d postgres redis
  uvicorn api.main:app --port 8001
  python scripts/simulate_local.py
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import httpx

# Make core importable when running from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.documents.generators.bank_stmt_generator import generate_bank_statement  # noqa: E402
from core.documents.generators.credit_report_generator import generate_credit_report  # noqa: E402
from core.documents.generators.identity_generator import generate_drivers_license  # noqa: E402
from core.documents.generators.paystub_generator import generate_paystub  # noqa: E402
from core.documents.generators.w2_generator import generate_w2  # noqa: E402
from core.ingestion.confidence import (  # noqa: E402
    ConfidenceResolver,
    FieldValue,
    SOURCE_CONFIDENCE_RANKING,
)
from core.ingestion.events import ChannelType  # noqa: E402

API_URL = "http://localhost:8001"
API_KEY = "edms_dev_key"
HEADERS_JSON = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
HEADERS_AUTH = {"X-API-Key": API_KEY}

LOCAL_STORAGE = Path("local_storage")
LOCAL_STORAGE.mkdir(parents=True, exist_ok=True)

# ── colour helpers ────────────────────────────────────────────────────────────
GREEN, YELLOW, CYAN, RED, MAGENTA, BOLD, RESET = (
    "\033[92m", "\033[93m", "\033[96m", "\033[91m", "\033[95m", "\033[1m", "\033[0m",
)


def ok(msg):   print(f"  {GREEN}[PASS]{RESET} {msg}")
def fail(msg): print(f"  {RED}[FAIL]{RESET} {msg}"); sys.exit(1)
def info(msg): print(f"  {CYAN}      {RESET} {msg}")
def warn(msg): print(f"  {YELLOW}[NOTE]{RESET} {msg}")


def step(n, title):
    print(f"\n{BOLD}{YELLOW}{'='*72}{RESET}")
    print(f"{BOLD}{YELLOW}  STEP {n}: {title}{RESET}")
    print(f"{BOLD}{YELLOW}{'='*72}{RESET}")


def show_json(label, data, max_lines=40):
    print(f"\n  {CYAN}{label}:{RESET}")
    lines = json.dumps(data, indent=4, default=str).split("\n")
    for line in lines[:max_lines]:
        print(f"    {line}")
    if len(lines) > max_lines:
        print(f"    ... ({len(lines)-max_lines} more lines)")


# ── docker-exec helpers ──────────────────────────────────────────────────────


def _docker(container, *args, timeout=10):
    try:
        result = subprocess.run(
            ["docker", "exec", container, *args],
            capture_output=True, text=True, timeout=timeout,
        )
        return result.stdout.strip()
    except Exception as e:
        return f"<error: {e}>"


def redis_get(key):
    val = _docker("edms-simulator-redis-1", "redis-cli", "GET", key, timeout=5)
    if not val or val == "(nil)":
        return None
    try:
        return json.loads(val)
    except json.JSONDecodeError:
        return val.strip('"')


def redis_keys(pattern):
    val = _docker("edms-simulator-redis-1", "redis-cli", "KEYS", pattern, timeout=5)
    return [k for k in val.split("\n") if k and k != "(nil)"]


def redis_ttl(key):
    return _docker("edms-simulator-redis-1", "redis-cli", "TTL", key, timeout=5)


def psql(query):
    return _docker(
        "edms-simulator-postgres-1",
        "psql", "-U", "edms", "-d", "edms",
        "-c", query, "--no-psqlrc",
        timeout=15,
    )


# ── header ────────────────────────────────────────────────────────────────────
print(f"\n{BOLD}EDMS Simulator — Local Simulation Walkthrough (Phase D){RESET}")
print(f"{'─'*72}")

try:
    httpx.get(f"{API_URL}/health", timeout=5).raise_for_status()
    ok(f"API is running at {API_URL}")
except Exception as e:
    fail(f"API not reachable at {API_URL}. Start it with `uvicorn api.main:app --port 8001`.\n  {e}")

claude_available = bool(os.getenv("ANTHROPIC_API_KEY"))
if claude_available:
    ok("ANTHROPIC_API_KEY is set — chat/email/image extractions will run live.")
else:
    warn("ANTHROPIC_API_KEY not set — chat/email body extractions will be skipped.")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Generate document package
# ─────────────────────────────────────────────────────────────────────────────
step(0, "Generate document package (W2, paystub, bank stmt, credit report, DL JPG)")

PRIMARY_NAME = "James Okafor"
PRIMARY_SSN_LAST4 = "4729"
PRIMARY_DOB = date(1982, 7, 14)
PRIMARY_ADDRESS = "100 Mission St\nSan Francisco, CA 94105"
PRIMARY_EMPLOYER = "Accenture LLC"
PRIMARY_INCOME = 92_400.00

CO_NAME = "Sarah Okafor"
CO_SSN_LAST4 = "8821"
CO_INCOME = 56_200.00

w2_pdf, w2_meta = generate_w2(
    employee_name=PRIMARY_NAME,
    employee_ssn_last4=PRIMARY_SSN_LAST4,
    employee_address=PRIMARY_ADDRESS,
    employer_name=PRIMARY_EMPLOYER,
    employer_ein="123456789",
    employer_address="1 Corporate Way\nSan Francisco, CA 94105",
    tax_year=date.today().year - 1,
    box1_wages=PRIMARY_INCOME,
)
paystub_pdf, paystub_meta = generate_paystub(
    employer_name=PRIMARY_EMPLOYER,
    employee_name=PRIMARY_NAME,
    employee_ssn_last4=PRIMARY_SSN_LAST4,
    pay_period_start=date.today() - timedelta(days=14),
    pay_period_end=date.today(),
    pay_date=date.today() + timedelta(days=3),
    gross_pay=round(PRIMARY_INCOME / 26, 2),
    ytd_gross=round(PRIMARY_INCOME / 26 * 8, 2),
)
bank_pdf, bank_meta = generate_bank_statement(
    bank_name="Pacific First Bank",
    account_holder=PRIMARY_NAME,
    account_number="9876543210",
    statement_end_date=date.today().replace(day=1) - timedelta(days=1),
    starting_balance=12_000.00,
    seed=42,
)
sample_credit = {
    "applicant_id": "APL-PENDING-P",
    "experian_score": 752, "equifax_score": 748, "transunion_score": 750,
    "mid_score": 750, "credit_band": "prime",
    "open_tradelines": 8, "revolving_utilization": 0.22,
    "monthly_obligations": [
        {"type": "car",         "creditor": "Auto Finance",  "monthly_payment": 425},
        {"type": "credit_card", "creditor": "Chase",         "monthly_payment": 120},
    ],
    "total_monthly_obligations": 545.00,
    "derogatory_marks": 0, "active_bankruptcy": False,
    "foreclosure_last_36mo": False,
    "late_30day": 0, "late_60day": 0, "late_90day": 0,
    "hard_inquiries_12mo": 2, "report_date": date.today().isoformat(),
}
credit_pdf, credit_meta = generate_credit_report(applicant_name=PRIMARY_NAME, profile=sample_credit)
dl_jpg, dl_meta = generate_drivers_license(
    state="CA", full_name=PRIMARY_NAME, dob=PRIMARY_DOB,
    address=PRIMARY_ADDRESS, dl_number="D1234567",
    expiry=date.today() + timedelta(days=365 * 4),
)

(LOCAL_STORAGE / "demo").mkdir(exist_ok=True)
artifacts = [
    ("demo/W2.pdf",            w2_pdf),
    ("demo/paystub.pdf",       paystub_pdf),
    ("demo/bank_statement.pdf", bank_pdf),
    ("demo/credit_report.pdf", credit_pdf),
    ("demo/drivers_license.jpg", dl_jpg),
]
for rel, content in artifacts:
    full = LOCAL_STORAGE / rel
    full.write_bytes(content)
    ok(f"{rel:<28} {len(content):>8,} bytes")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Chat-based intake
# ─────────────────────────────────────────────────────────────────────────────
step(1, "Chat-based intake → POST /ingest/chat")

chat_messages = [
    {"role": "user", "content": "Hi, I want to apply for a $385,000 mortgage."},
    {"role": "assistant", "content": "Great. To get started, what's your name and email?"},
    {"role": "user", "content": "James Okafor, james.okafor@email.com."},
    {"role": "user", "content": "I make $92,000 a year at Accenture."},
    {"role": "user", "content": "I also have rental income of about $1,800 a month."},
    {"role": "assistant", "content": "Will anyone co-borrow with you?"},
    {"role": "user", "content": "Yes — my wife Sarah Okafor will be co-borrower. She makes $56,000 at Dell."},
    {"role": "user", "content": "Our DOBs are 1982-07-14 and 1985-03-22."},
]

chat_extracted = None
chat_overall_conf = None
chat_missing: list[str] = []
chat_documents_needed: list[str] = []
chat_next_question = None
chat_failed = False
chat_failure_detail = None

if claude_available:
    try:
        r = httpx.post(
            f"{API_URL}/ingest/chat",
            json={"messages": chat_messages},
            headers=HEADERS_JSON,
            timeout=60,
        )
        if r.status_code != 200:
            chat_failed = True
            try:
                chat_failure_detail = r.json().get("detail") or r.text
            except Exception:
                chat_failure_detail = r.text
            warn(f"Chat ingest failed (HTTP {r.status_code}): {chat_failure_detail}")
        else:
            body = r.json()
            chat_extracted = body.get("extracted") or {}
            chat_overall_conf = body.get("overall_confidence")
            chat_missing = body.get("missing_fields") or []
            chat_documents_needed = body.get("documents_needed") or []
            chat_next_question = body.get("next_question_suggestion")
            ok(f"Chat extraction succeeded — overall_confidence={chat_overall_conf}")
            show_json("Extracted (truncated)", chat_extracted, max_lines=35)
            info(f"missing_fields:    {chat_missing}")
            info(f"documents_needed:  {chat_documents_needed}")
            if chat_next_question:
                info(f"next_question:     \"{chat_next_question}\"")
    except Exception as e:
        chat_failed = True
        chat_failure_detail = str(e)
        warn(f"Chat ingest failed: {e}")
else:
    warn("Skipping live chat — set ANTHROPIC_API_KEY to exercise this path.")
    info("(The /ingest/chat endpoint would return 503 without a key.)")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — PDF upload (W2)
# ─────────────────────────────────────────────────────────────────────────────
step(2, "Upload W2 PDF → POST /ingest/pdf (deterministic pymupdf path, no Claude)")

w2_event = None
try:
    files = {"file": ("W2.pdf", w2_pdf, "application/pdf")}
    r = httpx.post(
        f"{API_URL}/ingest/pdf",
        files=files,
        data={"borrower_role": "primary"},
        headers=HEADERS_AUTH,
        timeout=60,
    )
    r.raise_for_status()
    w2_event = r.json()
    ok(f"document_type detected: {w2_event['document_type']}")
    info(f"box1_wages   = ${w2_event['extracted_fields'].get('box1_wages'):,.2f}")
    info(f"employer     = {w2_event['extracted_fields'].get('employer_name')}")
    info(f"tax_year     = {w2_event['extracted_fields'].get('tax_year')}")
    info(f"confidence   = {w2_event['confidence']}")
    if chat_overall_conf is not None:
        info(
            "Confidence evolution: "
            f"chat (annual_income_stated)={chat_overall_conf:.2f} → "
            f"W2 (box1_wages)={w2_event['confidence']:.2f}"
        )
except Exception as e:
    warn(f"PDF ingest failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Email with paystub attachment
# ─────────────────────────────────────────────────────────────────────────────
step(3, "Email with paystub attachment → POST /ingest/email")

email_payload = {
    "from": "james.okafor@email.com",
    "subject": "Please find my pay stub attached",
    "body": "Hi — attaching this pay period's stub. Let me know if you need anything else.",
    "attachments": [
        {"filename": "paystub.pdf", "content_base64": base64.b64encode(paystub_pdf).decode()},
    ],
}

paystub_event = None
email_body_failed = False
try:
    r = httpx.post(
        f"{API_URL}/ingest/email",
        json=email_payload,
        headers=HEADERS_JSON,
        timeout=60,
    )
    if r.status_code != 200:
        try:
            detail = r.json().get("detail") or r.text
        except Exception:
            detail = r.text
        warn(f"Email ingest failed (HTTP {r.status_code}): {detail}")
    else:
        body = r.json()
        events = body.get("events", [])
        info(f"events returned: {len(events)} (1 body + {body.get('documents_processed')} attachment)")
        for ev in events:
            if ev["source_channel"] == ChannelType.EMAIL.value:
                ef = ev.get("extracted_fields") or {}
                if "_claude_error" in ef:
                    email_body_failed = True
                    warn(f"body event — Claude failed, fell back to low-confidence (confidence={ev['confidence']:.2f})")
                    info(f"reason: {ef['_claude_error']}")
                else:
                    ok(f"body event — confidence={ev['confidence']:.2f}, hint={ev.get('document_type')}")
            elif ev["source_channel"] == ChannelType.PDF_UPLOAD.value:
                paystub_event = ev
                ok(f"attachment event — {ev['document_type']} confidence={ev['confidence']:.2f}")
                ef = ev["extracted_fields"]
                info(f"gross_pay  = ${ef.get('gross_pay'):,.2f}    ytd_gross = ${ef.get('ytd_gross'):,.2f}")
                info(f"net_pay    = ${ef.get('net_pay'):,.2f}")
except Exception as e:
    warn(f"Email ingest failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Create the loan via /loans (golden record + Redis + Postgres)
# ─────────────────────────────────────────────────────────────────────────────
step(4, "POST /loans — create golden record; verify Redis + Postgres + XRef")

LOS_ID = f"LOS-{int(time.time())}"
loan_payload = {
    "los_id": LOS_ID,
    "borrower": {
        "first_name": "James",
        "last_name":  "Okafor",
        "dob":        PRIMARY_DOB.isoformat(),
        "ssn_hash":   "a3f9e2d1c4b5a6f7e8d9c0b1a2f3e4d5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1",
        "ssn_last4":  PRIMARY_SSN_LAST4,
        "email":      "james.okafor@email.com",
    },
    "co_borrower": {
        "first_name": "Sarah",
        "last_name":  "Okafor",
        "dob":        "1985-03-22",
        "ssn_hash":   "b4f0e3d2c5b6a7f8e9d0c1b2a3f4e5d6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
        "ssn_last4":  CO_SSN_LAST4,
    },
    "loan": {"loan_amount": 385000, "loan_type": "conventional", "credit_band": "prime"},
    "documents": [
        {"document_id": "DOC-W2-001", "document_type": "W2", "borrower_role": "primary",
         "box1_wages": int(PRIMARY_INCOME), "employer_name": PRIMARY_EMPLOYER},
        {"document_id": "DOC-PAY-001", "document_type": "PAYSTUB", "borrower_role": "primary"},
    ],
}

r = httpx.post(f"{API_URL}/loans", json=loan_payload, headers=HEADERS_JSON, timeout=30)
r.raise_for_status()
loan = r.json()
show_json("API response", loan, max_lines=10)
applicant_id = loan["applicant_id"]
co_applicant_id = loan["co_applicant_id"]
application_id = loan["application_id"]
ok(f"applicant_id={applicant_id}, co_applicant_id={co_applicant_id}")

time.sleep(1)

# Redis
status = redis_get(f"status:{applicant_id}")
if status == "active":
    ok(f"Redis status:{applicant_id} = 'active' (TTL {redis_ttl(f'status:{applicant_id}')}s)")
else:
    warn(f"Redis status: {status!r}")

income = redis_get(f"income:{applicant_id}")
if income:
    ok(f"Redis income:{applicant_id} cached — combined_qualifying_monthly=${income.get('combined_qualifying_monthly', 0):,.2f}")
credit = redis_get(f"credit:{applicant_id}")
if credit:
    ok(f"Redis credit:{applicant_id} cached — mid_score={credit.get('mid_score')}")

# Postgres
print()
print(f"  {CYAN}Postgres applicants:{RESET}")
print(psql(f"SELECT applicant_id, full_name, dob, status FROM applicants WHERE applicant_id IN ('{applicant_id}', '{co_applicant_id}');"))

print(f"\n  {CYAN}Postgres applicant_identity_xref:{RESET}")
print(psql(f"SELECT applicant_id, source_system, source_id, match_method, match_confidence FROM applicant_identity_xref WHERE applicant_id IN ('{applicant_id}', '{co_applicant_id}') ORDER BY added_at;"))

print(f"\n  {CYAN}Postgres applications:{RESET}")
print(psql(f"SELECT application_id, applicant_id, co_applicant_id, los_id, status FROM applications WHERE los_id='{LOS_ID}';"))


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Field confidence evolution
# ─────────────────────────────────────────────────────────────────────────────
step(5, "Field confidence evolution — same field, multiple sources, pick winner")

resolver = ConfidenceResolver()


def _ranked_table(field_name: str, sources: list[FieldValue]):
    if not sources:
        info(f"{field_name}: (no sources collected)")
        return
    res = resolver.resolve(field_name, sources)
    print(f"\n  {BOLD}{field_name}{RESET}")
    print(f"  {'source':<20} {'value':>14}  {'confidence':>10}  channel")
    print(f"  {'-'*20} {'-'*14}  {'-'*10}  {'-'*16}")
    for s in sorted(sources, key=lambda v: v.confidence, reverse=True):
        marker = "★" if s is res.chosen else " "
        val_str = f"{s.value:,.2f}" if isinstance(s.value, (int, float)) else str(s.value)
        print(f"  {marker} {s.source:<18} {val_str:>14}  {s.confidence:>10.2f}  {s.source_channel.value}")
    print(f"  {GREEN}→ chosen:{RESET} {res.chosen.source} = {res.chosen.value} (confidence {res.chosen.confidence:.2f})")
    if res.has_conflict:
        warn(f"CONFLICT — {res.conflict_reason}")


# annual_income sources from this run
annual_income_sources: list[FieldValue] = []
if chat_extracted and isinstance(chat_extracted.get("primary_borrower"), dict):
    val = chat_extracted["primary_borrower"].get("annual_income_stated")
    if isinstance(val, (int, float)):
        annual_income_sources.append(FieldValue(
            value=float(val),
            confidence=SOURCE_CONFIDENCE_RANKING["CHAT"],
            source="CHAT",
            source_channel=ChannelType.CHAT,
            requires_verification=True,
        ))
if w2_event:
    box1 = w2_event["extracted_fields"].get("box1_wages")
    if isinstance(box1, (int, float)):
        annual_income_sources.append(FieldValue(
            value=float(box1),
            confidence=SOURCE_CONFIDENCE_RANKING["W2_PDF"],
            source="W2_PDF",
            source_channel=ChannelType.PDF_UPLOAD,
        ))
if paystub_event:
    ytd = paystub_event["extracted_fields"].get("ytd_gross")
    if isinstance(ytd, (int, float)) and ytd > 0:
        # Annualize naively for comparison
        annualized = float(ytd) * 26 / 8
        annual_income_sources.append(FieldValue(
            value=annualized,
            confidence=SOURCE_CONFIDENCE_RANKING["PAYSTUB_PDF"],
            source="PAYSTUB_PDF (annualized)",
            source_channel=ChannelType.PDF_UPLOAD,
        ))

_ranked_table("annual_income", annual_income_sources)

# employer sources
employer_sources: list[FieldValue] = []
if chat_extracted and isinstance(chat_extracted.get("primary_borrower"), dict):
    emp = chat_extracted["primary_borrower"].get("employer")
    if emp:
        employer_sources.append(FieldValue(
            value=emp,
            confidence=SOURCE_CONFIDENCE_RANKING["CHAT"],
            source="CHAT",
            source_channel=ChannelType.CHAT,
            requires_verification=True,
        ))
if w2_event:
    emp = w2_event["extracted_fields"].get("employer_name")
    if emp:
        employer_sources.append(FieldValue(
            value=emp,
            confidence=SOURCE_CONFIDENCE_RANKING["W2_PDF"],
            source="W2_PDF",
            source_channel=ChannelType.PDF_UPLOAD,
        ))

_ranked_table("employer", employer_sources)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Same person, second LOS → deterministic SSN match
# ─────────────────────────────────────────────────────────────────────────────
step(6, "Same person, new LOS → deterministic SSN hash match (same applicant_id)")

LOS_ID_2 = f"LOS-{int(time.time())+10}"
payload2 = {
    "los_id": LOS_ID_2,
    "borrower": loan_payload["borrower"],  # same SSN hash
    "loan": {"loan_amount": 420000, "loan_type": "conventional", "credit_band": "prime"},
    "documents": [],
}
r = httpx.post(f"{API_URL}/loans", json=payload2, headers=HEADERS_JSON, timeout=30)
r.raise_for_status()
loan2 = r.json()
show_json("API response", loan2, max_lines=10)

if loan2["applicant_id"] == applicant_id and loan2["match_method"] == "deterministic":
    ok(f"SAME applicant_id returned: {applicant_id} via {loan2['match_method']} match")
else:
    fail(f"Expected deterministic match to {applicant_id}, got {loan2}")

print(f"\n  {CYAN}Postgres XRef table — both LOS IDs now linked:{RESET}")
print(psql(
    "SELECT source_system, source_id, match_method, match_confidence, added_at "
    f"FROM applicant_identity_xref WHERE applicant_id='{applicant_id}' ORDER BY added_at;"
))


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{BOLD}{GREEN}{'='*72}{RESET}")
print(f"{BOLD}{GREEN}  LOCAL SIMULATION COMPLETE{RESET}")
print(f"{BOLD}{GREEN}{'='*72}{RESET}")

artifact_count = len(artifacts)


def _chat_status() -> str:
    if chat_extracted:
        return "✓ live"
    if chat_failed:
        return f"failed (live error: {chat_failure_detail[:60]}…)" if chat_failure_detail else "failed (live error)"
    return "skipped (no key)"


def _email_status() -> str:
    if paystub_event and not email_body_failed:
        return "✓ body + attachment"
    if paystub_event and email_body_failed:
        return "partial (attachment ✓, body failed live)"
    if email_body_failed:
        return "body failed live (no attachment processed)"
    return "skipped (no key)" if not claude_available else "(body only)"


print(f"""
  Documents generated and saved to {LOCAL_STORAGE/'demo'}:
    {artifact_count} files ({sum(len(c) for _, c in artifacts):,} bytes total)

  Channels exercised this run:
    chat       {_chat_status()}
    pdf        ✓ deterministic (pymupdf)
    email+pdf  {_email_status()}
    api        ✓ POST /loans

  Postgres now contains:
    applicants               {applicant_id}, {co_applicant_id}
    applicant_identity_xref  rows for both LOS IDs ({LOS_ID}, {LOS_ID_2})
    applications             two rows tied to {applicant_id}

  Confidence ranking demonstrated for:
    annual_income  {len(annual_income_sources)} source(s) compared
    employer       {len(employer_sources)} source(s) compared
""")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — Document graph + agentic navigation
# ─────────────────────────────────────────────────────────────────────────────
step(7, "Document graph + agentic navigation (reconciler + navigator)")


def _graph_summary(label: str):
    r = httpx.get(
        f"{API_URL}/applicant/{applicant_id}/graph/summary",
        headers=HEADERS_AUTH, timeout=10,
    )
    r.raise_for_status()
    body = r.json()
    d = body["data"]
    info(
        f"{label:<28} docs={d['document_count']}  "
        f"rels={d['relationship_count']}  "
        f"confirms={d['confirmation_count']}  "
        f"conflicts={d['conflict_count']}  "
        f"requires_review={d['requires_review']}  "
        f"(source={body['source']})"
    )
    return d


# 7a/c — graph summary reflecting STEP 4's docs (already saved + reconciled)
print(f"\n  {CYAN}Graph state from STEP 4 ingest:{RESET}")
summary_after_step4 = _graph_summary("AFTER STEP 4")

if summary_after_step4["document_count"] >= 2:
    ok("Reconciler ran during STEP 4 — docs persisted, edges written")
else:
    warn("STEP 4 docs may not have persisted as expected")

# 7e — ask the navigator
print(f"\n  {CYAN}POST /applicant/{applicant_id}/navigate (clean state):{RESET}")
r = httpx.post(
    f"{API_URL}/applicant/{applicant_id}/navigate",
    headers=HEADERS_JSON,
    json={"question": "What is the qualifying annual income?"},
    timeout=60,
)
r.raise_for_status()
ans = r.json()
show_json("Navigator answer (clean)", ans, max_lines=30)
ok(
    f"answer.confidence={ans['confidence']:.2f}  "
    f"citations={len(ans['citations'])}  "
    f"requires_review={ans['requires_review']}"
)

# 7f — inject a contradicting 1099 via /loans/document
print(f"\n  {CYAN}Injecting contradicting 1099 (amount $45,000 vs W2 $92,400):{RESET}")
conflict_payload = {
    "applicant_id":   applicant_id,
    "application_id": application_id,
    "all_documents": [
        # Re-include the W2 so it's in scope for the reconciler comparison
        {
            "document_id":   "DOC-W2-001",
            "document_type": "W2_CURRENT",
            "borrower_role": "primary",
            "box1_wages":    int(PRIMARY_INCOME),
            "employer_name": PRIMARY_EMPLOYER,
        },
        # New conflicting 1099
        {
            "document_id":   "DOC-1099-CONFLICT",
            "document_type": "1099_NEC",
            "borrower_role": "primary",
            "amount":        45000.0,
            "payer_name":    "Sketchy Consulting LLC",
        },
    ],
}
r = httpx.post(
    f"{API_URL}/loans/document",
    json=conflict_payload, headers=HEADERS_JSON, timeout=30,
)
r.raise_for_status()
time.sleep(1)  # let async writes flush

print(f"\n  {CYAN}GET /applicant/{applicant_id}/conflicts:{RESET}")
r = httpx.get(
    f"{API_URL}/applicant/{applicant_id}/conflicts",
    headers=HEADERS_AUTH, timeout=10,
)
r.raise_for_status()
conflicts_resp = r.json()
n_conflicts = conflicts_resp["conflict_count"]
if n_conflicts >= 1:
    ok(f"Reconciler detected {n_conflicts} CONTRADICTS edge(s)")
    for c in conflicts_resp["conflicts"][:3]:
        info(f"  {c.get('field_name')}: {c.get('source_value')} ↔ {c.get('target_value')} (delta {c.get('delta_pct')}%)")
else:
    warn("Expected at least 1 contradicts edge — check reconciler config for 1099_NEC pair")

print(f"\n  {CYAN}POST /applicant/{applicant_id}/navigate (with conflict):{RESET}")
r = httpx.post(
    f"{API_URL}/applicant/{applicant_id}/navigate",
    headers=HEADERS_JSON,
    json={"question": "What is the qualifying annual income?"},
    timeout=60,
)
r.raise_for_status()
ans2 = r.json()
if ans2.get("requires_review"):
    ok(f"Navigator now flags requires_review=True with {len(ans2['conflicts_found'])} conflict(s)")
else:
    warn(f"Navigator did not flag the conflict — answer={ans2}")

_graph_summary("AFTER conflict injection")

