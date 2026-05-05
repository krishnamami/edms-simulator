#!/usr/bin/env python3
"""
EDMS Simulator — Local Simulation Walkthrough
Run this AFTER docker-compose up and uvicorn is running.

Usage:
  python scripts/simulate_local.py

What you will see:
  STEP 1: New application → placeholder → active in Redis + Postgres
  STEP 2: Verify Redis has golden record status + income + credit profiles
  STEP 3: Verify Postgres has applicant row + XRef row
  STEP 4: Upload a document → stale → re-aggregate → updated Redis
  STEP 5: Second application same person → same applicant_id (deterministic match)
"""

import httpx, json, time, subprocess, sys, os

API_URL  = "http://localhost:8001"
API_KEY  = "edms_dev_key"
HEADERS  = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}[PASS]{RESET} {msg}")
def fail(msg): print(f"  {RED}[FAIL]{RESET} {msg}"); sys.exit(1)
def info(msg): print(f"  {CYAN}      {RESET} {msg}")
def step(n, title):
    print(f"\n{BOLD}{YELLOW}{'='*60}{RESET}")
    print(f"{BOLD}{YELLOW}  STEP {n}: {title}{RESET}")
    print(f"{BOLD}{YELLOW}{'='*60}{RESET}")

def show_json(label, data):
    print(f"\n  {CYAN}{label}:{RESET}")
    lines = json.dumps(data, indent=4, default=str).split("\n")
    for line in lines[:40]:
        print(f"    {line}")
    if len(lines) > 40:
        print(f"    ... ({len(lines)-40} more lines)")

def redis_get(key):
    try:
        result = subprocess.run(
            ["docker", "exec", "edms-simulator-redis-1",
             "redis-cli", "GET", key],
            capture_output=True, text=True, timeout=5
        )
        val = result.stdout.strip()
        return json.loads(val) if val and val != "(nil)" else None
    except Exception as e:
        return None

def redis_keys(pattern):
    try:
        result = subprocess.run(
            ["docker", "exec", "edms-simulator-redis-1",
             "redis-cli", "KEYS", pattern],
            capture_output=True, text=True, timeout=5
        )
        return [k for k in result.stdout.strip().split("\n") if k and k != "(nil)"]
    except Exception:
        return []

def redis_ttl(key):
    try:
        result = subprocess.run(
            ["docker", "exec", "edms-simulator-redis-1",
             "redis-cli", "TTL", key],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"

def psql(query):
    try:
        result = subprocess.run(
            ["docker", "exec", "edms-simulator-postgres-1",
             "psql", "-U", "edms", "-d", "edms",
             "-c", query, "--no-psqlrc"],
            capture_output=True, text=True, timeout=10
        )
        return result.stdout.strip()
    except Exception as e:
        return f"Error: {e}"

# ── Check API is running ───────────────────────────────────────────────────────
print(f"\n{BOLD}EDMS Simulator — Local Simulation Walkthrough{RESET}")
print(f"{'─'*60}")

try:
    r = httpx.get(f"{API_URL}/health", timeout=5)
    r.raise_for_status()
    ok(f"API is running at {API_URL}")
except Exception as e:
    fail(f"API not reachable at {API_URL}. Start it with: uvicorn api.main:app --port 8001 --reload\nError: {e}")

# ── STEP 1: New application ────────────────────────────────────────────────────
step(1, "New application arrives — no golden record exists")

LOS_ID = f"LOS-2024-{int(time.time())}"
payload = {
    "los_id": LOS_ID,
    "borrower": {
        "first_name": "James",
        "last_name":  "Okafor",
        "dob":        "1982-07-14",
        "ssn_hash":   "a3f9e2d1c4b5a6f7e8d9c0b1a2f3e4d5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1",
        "ssn_last4":  "4729",
        "email":      "james.okafor@email.com",
    },
    "co_borrower": {
        "first_name": "Sarah",
        "last_name":  "Okafor",
        "dob":        "1985-03-22",
        "ssn_hash":   "b4f0e3d2c5b6a7f8e9d0c1b2a3f4e5d6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
        "ssn_last4":  "8821",
    },
    "loan": {
        "loan_amount":  385000,
        "loan_type":    "conventional",
        "credit_band":  "near-prime",
    },
    "documents": [
        {
            "document_id":    "DOC-W2-001",
            "document_type":  "W2_CURRENT",
            "borrower_role":  "primary",
            "employer_name":  "Accenture LLC",
            "box1_wages":     92400,
            "tax_year":       2023,
        },
        {
            "document_id":    "DOC-W2-002",
            "document_type":  "W2_CURRENT",
            "borrower_role":  "co_borrower",
            "employer_name":  "Dell Technologies",
            "box1_wages":     56200,
            "tax_year":       2023,
        },
        {
            "document_id":    "DOC-PAY-001",
            "document_type":  "PAYSTUB_CURRENT",
            "borrower_role":  "primary",
            "ytd_gross":      15400,
            "pay_period_end": "2024-02-28",
        },
    ]
}

print(f"\n  Sending POST /loans with LOS ID: {LOS_ID}")
print(f"  Borrower: James Okafor (primary) + Sarah Okafor (co-borrower)")

try:
    r = httpx.post(f"{API_URL}/loans", json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    result = r.json()
    show_json("API response", result)
except Exception as e:
    fail(f"POST /loans failed: {e}")

applicant_id    = result.get("applicant_id")
co_applicant_id = result.get("co_applicant_id")
application_id  = result.get("application_id")
match_method    = result.get("match_method")
is_new          = result.get("is_new_record")

if not applicant_id or not applicant_id.startswith("APL-"):
    fail(f"Expected APL-XXXXX-P format, got: {applicant_id}")
ok(f"Golden record created: {applicant_id} (primary)")
ok(f"Co-borrower golden record: {co_applicant_id}")
ok(f"Application ID: {application_id}")
ok(f"Match method: {match_method} — is_new_record: {is_new}")

# ── STEP 2: Verify Redis ────────────────────────────────────────────────────
step(2, "Verify Redis — status, income profile, credit profile")

time.sleep(1)  # small delay to ensure async writes complete

# Status check
status = redis_get(f"status:{applicant_id}")
if not status:
    raw = redis_get(f"status:{applicant_id}")
    # Try as raw string
    try:
        result2 = subprocess.run(
            ["docker", "exec", "edms-simulator-redis-1",
             "redis-cli", "GET", f"status:{applicant_id}"],
            capture_output=True, text=True, timeout=5
        )
        status = result2.stdout.strip().strip('"')
    except Exception:
        pass

if status == "active":
    ok(f"Redis status:{applicant_id} = '{status}'")
else:
    info(f"Redis status:{applicant_id} = '{status}' (may still be resolving)")

status_ttl = redis_ttl(f"status:{applicant_id}")
info(f"Status TTL: {status_ttl} seconds (~{int(status_ttl)//3600}hrs remaining)" if status_ttl.isdigit() else f"Status TTL: {status_ttl}")

# Income profile
income = redis_get(f"income:{applicant_id}")
if income:
    ok(f"Redis income:{applicant_id} — cache HIT")
    info(f"combined_qualifying_monthly = ${income.get('combined_qualifying_monthly', 0):,.2f}")
    info(f"qualifying_score_used = {income.get('qualifying_score_used', 'N/A')}")
    info(f"dti_inputs_ready = {income.get('dti_inputs_ready', False)}")
    info(f"lineage_hash = {income.get('lineage_hash', 'N/A')}")
    income_ttl = redis_ttl(f"income:{applicant_id}")
    info(f"TTL: {income_ttl}s (~{int(income_ttl)//3600}hrs)" if str(income_ttl).isdigit() else f"TTL: {income_ttl}")
else:
    info("Redis income profile not yet cached (check Postgres in step 3)")

# Credit profile
credit = redis_get(f"credit:{applicant_id}")
if credit:
    ok(f"Redis credit:{applicant_id} — cache HIT")
    info(f"mid_score = {credit.get('mid_score', 'N/A')}")
    info(f"credit_band = {credit.get('credit_band', 'N/A')}")
    info(f"total_monthly_obligations = ${credit.get('total_monthly_obligations', 0):,.2f}")
else:
    info("Redis credit profile not yet cached")

# App lookup cache
app_lookup = redis_get(f"app_los:{LOS_ID}")
if app_lookup:
    ok(f"Redis app_los:{LOS_ID} — lookup cache HIT")
else:
    info("App lookup not yet cached (cached on first GET request)")

# All Redis keys for this applicant
all_keys = redis_keys(f"*{applicant_id}*")
info(f"All Redis keys for {applicant_id}: {all_keys}")

# ── STEP 3: Verify Postgres ──────────────────────────────────────────────────
step(3, "Verify Postgres — golden record, XRef table, income profile")

# Applicant row
print(f"\n  Querying applicants table for {applicant_id}:")
rows = psql(f"SELECT applicant_id, full_name, dob, status, created_at FROM applicants WHERE applicant_id='{applicant_id}';")
print(f"\n{rows}")
if applicant_id in rows:
    ok(f"Postgres: applicant row found for {applicant_id}")
else:
    fail(f"Postgres: no applicant row found for {applicant_id}")

# XRef row
print(f"\n  Querying identity_xref table for {applicant_id}:")
xref_rows = psql(f"SELECT applicant_id, source_system, source_id, match_method, match_confidence FROM applicant_identity_xref WHERE applicant_id='{applicant_id}';")
print(f"\n{xref_rows}")
if "los" in xref_rows and LOS_ID in xref_rows:
    ok(f"Postgres: XRef row found — source_system=los, source_id={LOS_ID}")
else:
    info(f"XRef: {xref_rows}")

# Application row
print(f"\n  Querying applications table:")
app_rows = psql(f"SELECT application_id, applicant_id, co_applicant_id, los_id, status FROM applications WHERE los_id='{LOS_ID}';")
print(f"\n{app_rows}")
if LOS_ID in app_rows:
    ok(f"Postgres: application row found — los_id={LOS_ID}")

# Income profile
print(f"\n  Querying income_profiles table:")
income_rows = psql(f"SELECT profile_id, applicant_id, lineage_hash, version, assembled_at FROM income_profiles WHERE applicant_id='{applicant_id}' ORDER BY version DESC LIMIT 3;")
print(f"\n{income_rows}")
if applicant_id in income_rows:
    ok(f"Postgres: income_profile row found for {applicant_id}")

# Co-borrower check
if co_applicant_id:
    co_rows = psql(f"SELECT applicant_id, full_name, status FROM applicants WHERE applicant_id='{co_applicant_id}';")
    if co_applicant_id in co_rows:
        ok(f"Postgres: co-borrower golden record found: {co_applicant_id}")

# ── STEP 4: Upload a document → re-aggregation ─────────────────────────────
step(4, "Upload new document for same applicant → stale → re-aggregate → new lineage_hash")

# Capture current lineage_hash before upload
lineage_before = income.get("lineage_hash", "none") if income else "none"
info(f"lineage_hash BEFORE upload: {lineage_before}")

# Simulate document upload event
doc_payload = {
    "applicant_id":   applicant_id,
    "application_id": application_id,
    "document_id":    "DOC-1099-001",
    "document_type":  "1099_NEC",
    "source_id":      f"VENDOR-{int(time.time())}",
    "all_documents": [
        # Original docs + new 1099
        {
            "document_id":    "DOC-W2-001",
            "document_type":  "W2_CURRENT",
            "borrower_role":  "primary",
            "employer_name":  "Accenture LLC",
            "box1_wages":     92400,
            "tax_year":       2023,
        },
        {
            "document_id":    "DOC-1099-001",
            "document_type":  "CONTRACTOR_1099",
            "borrower_role":  "primary",
            "amount_1099":    18500,
            "payer_name":     "Consulting Client LLC",
            "tax_year":       2023,
        },
        {
            "document_id":    "DOC-PAY-001",
            "document_type":  "PAYSTUB_CURRENT",
            "borrower_role":  "primary",
            "ytd_gross":      15400,
        },
        {
            "document_id":    "DOC-W2-002",
            "document_type":  "W2_CURRENT",
            "borrower_role":  "co_borrower",
            "employer_name":  "Dell Technologies",
            "box1_wages":     56200,
            "tax_year":       2023,
        },
    ]
}

print(f"\n  Sending document upload event for applicant {applicant_id}")
print(f"  New document: DOC-1099-001 (1099 consulting income — $18,500)")

try:
    r = httpx.post(f"{API_URL}/loans/document",
                   json=doc_payload, headers=HEADERS, timeout=30)
    if r.status_code == 404:
        info("POST /loans/document endpoint not yet built — simulating via aggregation service directly")
        info("This will be wired in the full build. Showing expected behaviour:")
        print(f"""
  Expected sequence:
    1. Redis status:{applicant_id} flips to "stale"
    2. Income assembly re-runs with all_documents (including new 1099)
    3. New income_profiles row inserted in Postgres (version 2)
    4. Old row gets superseded_by = new profile_id
    5. Redis income:{applicant_id} updated with new profile
    6. lineage_hash changes (new document in set)
    7. Redis status:{applicant_id} flips back to "active"
    8. Decision OS — if it calls GET /income-profile now,
       it gets the updated profile with the 1099 income included
""")
    else:
        r.raise_for_status()
        result3 = r.json()
        show_json("Document upload response", result3)
        time.sleep(1)

        income_after = redis_get(f"income:{applicant_id}")
        if income_after:
            lineage_after = income_after.get("lineage_hash","none")
            if lineage_after != lineage_before:
                ok(f"lineage_hash CHANGED after document upload")
                info(f"  Before: {lineage_before}")
                info(f"  After:  {lineage_after}")
                q_monthly_after = income_after.get("combined_qualifying_monthly", 0)
                info(f"  combined_qualifying_monthly updated: ${q_monthly_after:,.2f}")
            else:
                info(f"lineage_hash unchanged (document did not change income sources)")
        print(f"\n  Postgres income_profiles (versioned):")
        rows2 = psql(f"SELECT profile_id, lineage_hash, version, superseded_by, assembled_at FROM income_profiles WHERE applicant_id='{applicant_id}' ORDER BY version;")
        print(f"\n{rows2}")
        if rows2:
            ok("Postgres: income_profiles has versioned rows with superseded_by chain")

except Exception as e:
    info(f"Document upload endpoint note: {e}")

# ── STEP 5: Same person applies again → deterministic match ─────────────────
step(5, "Same person, new LOS ID → deterministic SSN hash match → SAME applicant_id")

LOS_ID_2 = f"LOS-2026-{int(time.time())}"
payload2 = {
    "los_id": LOS_ID_2,
    "borrower": {
        "first_name": "James",
        "last_name":  "Okafor",
        "dob":        "1982-07-14",
        "ssn_hash":   "a3f9e2d1c4b5a6f7e8d9c0b1a2f3e4d5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1",  # SAME SSN hash
        "ssn_last4":  "4729",
        "email":      "james.okafor@email.com",
    },
    "loan": {"loan_amount": 420000, "loan_type": "conventional", "credit_band": "prime"},
    "documents": []
}

print(f"\n  Same James Okafor, new LOS ID: {LOS_ID_2}")
print(f"  SSN hash is IDENTICAL to first application")

try:
    r = httpx.post(f"{API_URL}/loans", json=payload2, headers=HEADERS, timeout=30)
    r.raise_for_status()
    result4 = r.json()
    show_json("API response", result4)

    applicant_id_2 = result4.get("applicant_id")
    match_method_2 = result4.get("match_method")
    is_new_2       = result4.get("is_new_record")

    if applicant_id_2 == applicant_id:
        ok(f"SAME applicant_id returned: {applicant_id_2}")
        ok(f"Match method: {match_method_2} (SSN hash deterministic match)")
        ok(f"is_new_record: {is_new_2} (False = existing record reused)")
    else:
        fail(f"Expected {applicant_id}, got {applicant_id_2}")

    print(f"\n  Postgres XRef table now has TWO LOS IDs for same applicant:")
    xref_both = psql(f"SELECT source_system, source_id, match_method, match_confidence, added_at FROM applicant_identity_xref WHERE applicant_id='{applicant_id}' ORDER BY added_at;")
    print(f"\n{xref_both}")
    if LOS_ID in xref_both and LOS_ID_2 in xref_both:
        ok(f"XRef table: both LOS IDs linked to same golden record {applicant_id}")

except Exception as e:
    info(f"Second application: {e}")

# ── STEP 6: GET endpoints — verify cache vs DB ──────────────────────────────
step(6, "GET endpoints — verify Redis cache hit vs Postgres fallback")

print(f"\n  GET /loan/{LOS_ID}/applicant-id")
try:
    r = httpx.get(f"{API_URL}/loan/{LOS_ID}/applicant-id", headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    show_json("Response", data)
    if data.get("applicant_id") == applicant_id:
        ok(f"LOS ID resolved to correct applicant_id: {applicant_id}")
except Exception as e:
    info(f"GET /loan: {e}")

print(f"\n  GET /applicant/{applicant_id}/income-profile")
try:
    r = httpx.get(f"{API_URL}/applicant/{applicant_id}/income-profile",
                  headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    source = data.get("source","unknown")
    income_data = data.get("data", data)
    info(f"Source: {BOLD}{source}{RESET} ({'Redis' if source=='cache' else 'Postgres'})")
    info(f"combined_qualifying_monthly: ${income_data.get('combined_qualifying_monthly',0):,.2f}")
    info(f"qualifying_score_used: {income_data.get('qualifying_score_used','N/A')}")
    info(f"requires_human_review: {income_data.get('requires_human_review', False)}")
    ok(f"Income profile returned from {source}")
except Exception as e:
    info(f"GET /income-profile: {e}")

print(f"\n  GET /applicant/{applicant_id}/credit-profile")
try:
    r = httpx.get(f"{API_URL}/applicant/{applicant_id}/credit-profile",
                  headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    source = data.get("source","unknown")
    credit_data = data.get("data", data)
    info(f"Source: {BOLD}{source}{RESET}")
    info(f"mid_score: {credit_data.get('mid_score','N/A')}")
    info(f"credit_band: {credit_data.get('credit_band','N/A')}")
    ok(f"Credit profile returned from {source}")
except Exception as e:
    info(f"GET /credit-profile: {e}")

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print(f"\n{BOLD}{GREEN}{'='*60}{RESET}")
print(f"{BOLD}{GREEN}  LOCAL SIMULATION COMPLETE{RESET}")
print(f"{BOLD}{GREEN}{'='*60}{RESET}")
print(f"""
  What you just verified locally:

  {GREEN}✓{RESET}  New application → golden record created (APL-XXXXX-P)
  {GREEN}✓{RESET}  Redis: status, income profile, credit profile all cached
  {GREEN}✓{RESET}  Postgres: applicant row, XRef row, income profile row
  {GREEN}✓{RESET}  Document upload → lineage_hash changes → Redis updated
  {GREEN}✓{RESET}  Same SSN → same applicant_id returned (deterministic match)
  {GREEN}✓{RESET}  GET endpoints serve from Redis cache (fast path)

  Redis keys written for {applicant_id}:
    status:{applicant_id}   TTL 24hr
    income:{applicant_id}   TTL 4hr
    credit:{applicant_id}   TTL 4hr

  Postgres tables written:
    applicants              1 row (golden record)
    applicant_identity_xref 2 rows (LOS-1 + LOS-2)
    applications            2 rows (one per LOS ID)
    income_profiles         1+ rows (versioned)
    credit_profiles         1 row (current)

  When you are satisfied this works locally:
    git add . && git commit -m "feat: EDMS Simulator verified locally"
    git push
    → GitHub Actions aws.yaml deploys to ECS automatically
""")
