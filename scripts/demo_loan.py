#!/usr/bin/env python3
"""
EDMS Single Loan Simulation
Drops W2s, credit report, and appraisal into S3
then shows the full pipeline to Redis.

Scenario:
  Loan: LOS-DEMO-001
  Primary borrower:    James Okafor   (W2 from Accenture)
  Co-borrower:         Sarah Okafor   (W2 from Dell)
  Credit report:       Joint tri-merge (both borrowers)
  Property appraisal:  123 Oak Street, Austin TX

Usage:
  # Local (docker-compose)
  python scripts/demo_loan.py

  # Production AWS
  python scripts/demo_loan.py --live

  # Step by step (pause between each drop)
  python scripts/demo_loan.py --step
"""

import argparse, boto3, httpx, json, os, pathlib, time

# ── Config ────────────────────────────────────────────────────────────────────

PROD_URL      = "http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com"
LOCAL_URL     = "http://localhost:8001"
S3_BUCKET     = "edms-simulator-loans"
LOCAL_S3      = pathlib.Path(os.getenv("LOCAL_STORAGE_PATH", "./local_storage"))
PROD_API_KEY  = "edms-prod-key-2026"
LOCAL_API_KEY = "edms_dev_key"

# ── Colour output ─────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}✓{RESET}  {msg}")
def fail(msg):  print(f"  {RED}✗{RESET}  {msg}")
def info(msg):  print(f"  {CYAN}→{RESET}  {msg}")
def warn(msg):  print(f"  {YELLOW}!{RESET}  {msg}")
def blank():    print()

def banner(title, char="═"):
    width = 60
    print(f"\n{BOLD}{char * width}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{char * width}{RESET}")

def section(title):
    print(f"\n{CYAN}{BOLD}  ── {title} ──{RESET}")

# ── Loan definition ───────────────────────────────────────────────────────────

LOS_ID = "LOS-DEMO-001"

LOAN = {
    "los_id": LOS_ID,
    "borrower": {
        "first_name": "James",
        "last_name":  "Okafor",
        "dob":        "1982-07-14",
        "ssn_hash":   "a3f9e2d1c4b5a6f7e8d9c0b1a2f3e4d5"
                      "a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1",
        "ssn_last4":  "4729",
        "email":      "james.okafor@email.com",
    },
    "co_borrower": {
        "first_name": "Sarah",
        "last_name":  "Okafor",
        "dob":        "1985-03-22",
        "ssn_hash":   "b4f0e3d2c5b6a7f8e9d0c1b2a3f4e5d6"
                      "a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
        "ssn_last4":  "8821",
        "email":      "sarah.okafor@email.com",
    },
    "loan": {
        "loan_amount":     385000,
        "loan_type":       "conventional",
        "credit_band":     "near-prime",
        "interest_rate":   7.0,
        "loan_term_months": 360,
        "loan_purpose":    "purchase",
        "occupancy":       "primary_residence",
    },
    "documents": [],
}

# ── Documents to drop ─────────────────────────────────────────────────────────
# Dropped in this order:
#   1. W2 primary borrower   → income assembled for James
#   2. W2 co-borrower        → income re-assembled (combined)
#   3. Credit report         → credit profile cached in Redis
#   4. Appraisal             → property profile + LTV calculation

DOCUMENTS = [
    {
        "step":        1,
        "label":       "W2 — James Okafor (Primary Borrower)",
        "type":        "W2_CURRENT",
        "category":    "income",
        "role":        "primary",
        "filename":    "w2_primary.pdf",
        "data": {
            "employer_name":   "Accenture LLC",
            "box1_wages":      92400,
            "box2_fed_tax":    18234,
            "box3_ss_wages":   92400,
            "box4_ss_tax":     5729,
            "box5_med_wages":  92400,
            "box6_med_tax":    1340,
            "tax_year":        2023,
            "ssn_last4":       "4729",
            "state":           "TX",
            "employer_ein":    "12-3456789",
        },
        "what_happens": [
            "W2 extracted → employer=Accenture, wages=$92,400",
            "document_index row written (confidence 0.94)",
            "Income assembler runs → qualifying=$7,700/mo",
            "Redis income:APL-DEMO-001-P cached (TTL 4hr)",
        ],
    },
    {
        "step":        2,
        "label":       "W2 — Sarah Okafor (Co-Borrower)",
        "type":        "W2_CURRENT",
        "category":    "income",
        "role":        "co_borrower",
        "filename":    "w2_coborrower.pdf",
        "data": {
            "employer_name":   "Dell Technologies",
            "box1_wages":      56200,
            "box2_fed_tax":    9872,
            "box3_ss_wages":   56200,
            "box4_ss_tax":     3484,
            "box5_med_wages":  56200,
            "box6_med_tax":    815,
            "tax_year":        2023,
            "ssn_last4":       "8821",
            "state":           "TX",
            "employer_ein":    "98-7654321",
        },
        "what_happens": [
            "Co-borrower W2 extracted → employer=Dell, wages=$56,200",
            "document_index row written (confidence 0.94)",
            "Income assembler re-runs → combined=$12,350/mo",
            "Redis income:APL-DEMO-001-P UPDATED (combined income)",
            "Graph: CONFIRMS employer field across both W2s",
        ],
    },
    {
        "step":        3,
        "label":       "Credit Report — Joint Tri-Merge",
        "type":        "CREDIT_REPORT",
        "category":    "credit",
        "role":        "primary",
        "filename":    "credit_report.pdf",
        "data": {
            "experian_score":             732,
            "equifax_score":              721,
            "transunion_score":           723,
            "mid_score":                  723,
            "credit_band":                "near-prime",
            "open_tradelines":            15,
            "revolving_utilization":      0.38,
            "monthly_obligations":        944,
            "derogatory_marks":           0,
            "active_bankruptcy":          False,
            "foreclosure_last_36mo":      False,
            "late_30day":                 0,
            "late_60day":                 0,
            "late_90day":                 0,
            "hard_inquiries_12mo":        2,
            "pull_type":                  "hard",
            "report_date":                "2026-05-07",
            "monthly_obligations_detail": [
                {"type": "car",         "creditor": "Auto Finance",  "payment": 381},
                {"type": "student",     "creditor": "Student Loans", "payment": 315},
                {"type": "credit_card", "creditor": "Chase Visa",    "payment": 248},
            ],
        },
        "what_happens": [
            "Credit report extracted → mid_score=723, near-prime",
            "Monthly obligations=$944/mo (car+student+CC)",
            "document_index row written (confidence 0.95)",
            "Credit assembler runs → credit profile assembled",
            "Redis credit:APL-DEMO-001-P cached (TTL 4hr)",
        ],
    },
    {
        "step":        4,
        "label":       "Property Appraisal — 123 Oak Street Austin TX",
        "type":        "APPRAISAL_URAR",
        "category":    "property",
        "role":        "primary",
        "filename":    "appraisal_urar.pdf",
        "data": {
            "property_address":  "123 Oak Street, Austin TX 78701",
            "appraised_value":   485000,
            "opinion_of_value":  485000,
            "condition_rating":  "C2",
            "effective_date":    "2026-05-06",
            "appraisal_date":    "2026-05-06",
            "property_type":     "Single Family Residence",
            "year_built":        2005,
            "gross_living_area": 2450,
            "bedrooms":          4,
            "bathrooms":         2.5,
            "appraiser_name":    "John Smith MAI",
            "appraiser_license": "TX-1234567",
            "comparable_1":      {"address": "125 Oak St", "sale_price": 478000},
            "comparable_2":      {"address": "210 Elm Ave", "sale_price": 492000},
            "comparable_3":      {"address": "88 Maple Dr", "sale_price": 481000},
            "ltv_at_appraised":  round(385000 / 485000 * 100, 2),
        },
        "what_happens": [
            "Appraisal extracted → appraised_value=$485,000, condition=C2",
            "LTV calculated → 79.4% (conforming, no PMI required)",
            "document_index row written (confidence 0.97)",
            "Property profile assembled (if Phase B built)",
            "Redis property:PROP-DEMO-001 cached",
            "Graph: no conflicts (first property doc)",
        ],
    },
]

# ── Minimal PDF generator — instant, no external libs ─────────────────────────

def make_pdf(data: dict, title: str) -> bytes:
    """
    Generate a minimal valid PDF with data embedded as text.
    No reportlab. No PyMuPDF. Instant. Never hangs.
    """
    lines = [title, ""]
    for k, v in data.items():
        if isinstance(v, (dict, list)):
            lines.append(f"{k}: {json.dumps(v)}")
        else:
            lines.append(f"{k}: {v}")

    content = "\n".join(lines)
    content_bytes = content.encode("latin-1", errors="replace")

    stream = (
        b"BT\n/F1 10 Tf\n50 750 Td\n12 TL\n"
        + b"(" + content_bytes[:3000] + b") Tj\n"
        + b"ET"
    )

    obj1 = b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    obj2 = b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
    obj3 = (b"3 0 obj\n<< /Type /Page /Parent 2 0 R "
            b"/MediaBox [0 0 612 792] /Contents 4 0 R "
            b"/Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n")
    obj4 = (b"4 0 obj\n<< /Length " + str(len(stream)).encode() + b" >>\n"
            b"stream\n" + stream + b"\nendstream\nendobj\n")
    obj5 = (b"5 0 obj\n<< /Type /Font /Subtype /Type1 "
            b"/BaseFont /Helvetica >>\nendobj\n")

    body    = b"%PDF-1.4\n" + obj1 + obj2 + obj3 + obj4 + obj5
    xref_offset = len(body)

    offsets = []
    pos = 9  # after "%PDF-1.4\n"
    for obj in [obj1, obj2, obj3, obj4, obj5]:
        offsets.append(pos)
        pos += len(obj)

    xref  = b"xref\n0 6\n0000000000 65535 f \n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \n".encode()

    trailer = (
        b"trailer\n<< /Size 6 /Root 1 0 R >>\n"
        b"startxref\n" + str(xref_offset).encode() + b"\n%%EOF\n"
    )
    return body + xref + trailer

# ── S3 / local storage ────────────────────────────────────────────────────────

def drop_file(key: str, content: bytes,
              live: bool, s3=None) -> int:
    if live and s3:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key, Body=content,
            ContentType="application/pdf",
            ServerSideEncryption="aws:kms",
            Metadata={"simulation": "demo", "los_id": LOS_ID},
        )
    else:
        path = LOCAL_S3 / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return len(content)

def read_file(key: str, live: bool, s3=None) -> bytes:
    if live and s3:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return resp["Body"].read()
    else:
        return (LOCAL_S3 / key).read_bytes()

def list_files(prefix: str, live: bool, s3=None) -> list:
    results = []
    if live and s3:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                results.append({
                    "key":  obj["Key"],
                    "size": obj["Size"],
                })
    else:
        base = LOCAL_S3 / prefix.lstrip("/")
        if base.exists():
            for f in sorted(base.rglob("*")):
                if f.is_file():
                    rel = str(f.relative_to(LOCAL_S3)).replace("\\", "/")
                    results.append({
                        "key":  rel,
                        "size": f.stat().st_size,
                    })
    return results

# ── API helpers ───────────────────────────────────────────────────────────────

def api(method, path, base_url, api_key, **kwargs):
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    return httpx.request(
        method, f"{base_url}{path}",
        headers=headers, timeout=15, **kwargs
    )

# ── Pipeline state reader ─────────────────────────────────────────────────────

def show_redis_state(applicant_id: str, co_applicant_id: str,
                     application_id: str,
                     base_url: str, api_key: str,
                     label: str = "CURRENT STATE"):
    section(label)

    # Income profile (Redis → Postgres)
    r = api("GET", f"/applicant/{applicant_id}/income-profile",
            base_url, api_key)
    if r.status_code == 200:
        body   = r.json()
        source = body.get("source", "?")
        data   = body.get("data", body)
        qm     = (data.get("combined_qualifying_monthly") or
                  data.get("primary_borrower", {}).get("qualifying_monthly", 0))
        color  = GREEN if source == "cache" else YELLOW
        print(f"\n  income:{applicant_id}")
        print(f"    source:              {color}{source}{RESET}")
        print(f"    qualifying_monthly:  ${float(qm):,.0f}")
        pb = data.get("primary_borrower", {})
        if pb.get("sources"):
            for src in pb["sources"]:
                print(f"    source_doc:          "
                      f"{src.get('document_type','?')}  "
                      f"${src.get('annual_income',0)/12:,.0f}/mo  "
                      f"conf={src.get('confidence',0):.2f}")
    else:
        print(f"\n  income:{applicant_id}  → not yet assembled")

    # Credit profile
    r2 = api("GET", f"/applicant/{applicant_id}/credit-profile",
             base_url, api_key)
    if r2.status_code == 200:
        body   = r2.json()
        source = body.get("source", "?")
        data   = body.get("data", body)
        color  = GREEN if source == "cache" else YELLOW
        print(f"\n  credit:{applicant_id}")
        print(f"    source:              {color}{source}{RESET}")
        print(f"    mid_score:           {data.get('mid_score','?')}")
        print(f"    credit_band:         {data.get('credit_band','?')}")
        print(f"    monthly_obligations: "
              f"${data.get('total_monthly_obligations',0):,.0f}/mo")
    else:
        print(f"\n  credit:{applicant_id}  → not yet assembled")

    # Graph summary
    r3 = api("GET", f"/applicant/{applicant_id}/graph/summary",
             base_url, api_key)
    if r3.status_code == 200:
        data = r3.json().get("data", r3.json())
        print(f"\n  graph:{applicant_id}")
        print(f"    documents:     {data.get('document_count', 0)}")
        print(f"    edges:         {data.get('relationship_count', 0)}")
        print(f"    confirms:      {data.get('confirmation_count', 0)}")
        print(f"    conflicts:     {data.get('conflict_count', 0)}")

    # Graph edges (only if any)
    r4 = api("GET", f"/applicant/{applicant_id}/graph",
             base_url, api_key)
    if r4.status_code == 200:
        rels = r4.json().get("relationships", [])
        if rels:
            print(f"\n  graph_edges:")
            for rel in rels:
                t = rel.get("relationship_type", "?")
                c = (GREEN  if t == "confirms"
                     else YELLOW if t == "corroborates"
                     else RED    if t == "contradicts"
                     else CYAN)
                icon = ("✓" if t == "confirms"
                        else "≈" if t == "corroborates"
                        else "✗" if t == "contradicts"
                        else "→")
                print(f"    {c}{icon} {t.upper():<15}{RESET}  "
                      f"field={rel.get('field_name','?')[:30]}  "
                      f"conf={rel.get('confidence',0):.2f}")

# ── S3 file viewer ────────────────────────────────────────────────────────────

def show_file_content(key: str, live: bool, s3=None):
    section(f"S3 File Content: {key}")
    try:
        content = read_file(key, live, s3)
        size    = len(content)
        print(f"\n  File size: {size:,} bytes")
        print(f"  Type:      PDF")
        print(f"\n  Embedded data (extracted from PDF):")
        text = content.decode("latin-1", errors="ignore")
        for line in text.split("\n"):
            line = line.strip()
            if ": " in line and not line.startswith("%") and \
               not line.startswith("/") and not line.startswith("<<") and \
               len(line) < 120:
                print(f"    {CYAN}{line}{RESET}")
    except Exception as e:
        warn(f"Could not read file: {e}")

# ── Main simulation ───────────────────────────────────────────────────────────

def run(base_url: str, api_key: str, live: bool,
        step_mode: bool, s3=None):

    banner("EDMS SINGLE LOAN SIMULATION")
    print(f"\n  Loan:        {LOS_ID}")
    print(f"  Primary:     James Okafor  (W2 Accenture $92,400/yr)")
    print(f"  Co-borrower: Sarah Okafor  (W2 Dell $56,200/yr)")
    print(f"  Combined:    $148,600/yr → $12,383/mo qualifying")
    print(f"  Property:    123 Oak Street, Austin TX")
    print(f"  Loan:        $385,000 @ 7.0% 30yr")
    print(f"  Appraisal:   $485,000  LTV=79.4%")
    print(f"\n  Mode:        {'PRODUCTION AWS' if live else 'LOCAL'}")
    print(f"  API:         {base_url}")

    # ── Pre-flight check ──────────────────────────────────────────────────
    banner("PRE-FLIGHT", "─")
    r = httpx.get(f"{base_url}/health",
                  headers={"X-API-Key": api_key}, timeout=10)
    if r.status_code == 200:
        ok(f"API healthy")
    else:
        fail(f"API not reachable: {r.status_code}")
        return

    r2 = httpx.get(f"{base_url}/ready",
                   headers={"X-API-Key": api_key}, timeout=10)
    if r2.status_code == 200:
        ready = r2.json()
        pg    = ready.get("postgres", False)
        rd    = ready.get("redis", False)
        ok(f"Postgres: {'✓' if pg else '✗'}  Redis: {'✓' if rd else '✗'}")
        if not pg or not rd:
            fail("Stack not fully ready")
            return
    else:
        warn(f"/ready: {r2.status_code}")

    # ── Create loan application ───────────────────────────────────────────
    banner("STEP 0 — CREATE LOAN APPLICATION", "─")
    info(f"POST /loans  {LOS_ID}")

    r3 = api("POST", "/loans", base_url, api_key, json=LOAN)
    if r3.status_code != 200:
        fail(f"Create loan failed: {r3.status_code} {r3.text[:80]}")
        return

    result         = r3.json()
    application_id = result["application_id"]
    applicant_id   = result["applicant_id"]
    co_id          = result.get("co_applicant_id", "")

    ok(f"Application:  {application_id}")
    ok(f"Primary:      {applicant_id} "
       f"({result.get('match_method','?')} — "
       f"{'new record' if result.get('is_new_record') else 'existing'})")
    if co_id:
        ok(f"Co-borrower:  {co_id}")

    blank()
    info("Redis state BEFORE any documents:")
    show_redis_state(applicant_id, co_id, application_id,
                     base_url, api_key, "INITIAL STATE")

    # ── Drop each document ────────────────────────────────────────────────
    for doc in DOCUMENTS:
        banner(f"STEP {doc['step']} — {doc['label']}")

        # What will happen
        info("What this document does:")
        for line in doc["what_happens"]:
            print(f"    {DIM}• {line}{RESET}")
        blank()

        if step_mode:
            input(f"  {YELLOW}Press ENTER to drop this file...{RESET}")

        # Generate PDF
        pdf_bytes = make_pdf(doc["data"], title=doc["label"])
        key       = (f"loans/{LOS_ID}/{doc['category']}/"
                     f"{doc['filename']}")

        # Drop into S3
        size = drop_file(key, pdf_bytes, live, s3)
        ok(f"Dropped into S3: {key}  ({size:,} bytes)")

        # Show file content
        show_file_content(key, live, s3)

        # Feed into pipeline via API
        blank()
        info("Feeding into indexing pipeline...")

        doc_id  = f"DOC-{LOS_ID}-{doc['type']}-{doc['role']}"
        # IncomeAssembler reads box1_wages / gross_pay_ytd / etc. directly off
        # the doc dict (core/income/rules.py:30), so the extracted fields must
        # live at TOP LEVEL — nesting them under "extracted_fields" yields 0.
        payload = {
            "applicant_id":   applicant_id,
            "application_id": application_id,
            "all_documents": [{
                "document_id":       doc_id,
                "document_type":     doc["type"],
                "document_category": doc["category"],
                "borrower_role":     doc["role"],
                "s3_key":            key,
                "confidence_score":  (0.97 if doc["type"] == "APPRAISAL_URAR"
                                      else 0.95 if doc["type"] == "CREDIT_REPORT"
                                      else 0.94),
                "status":            "indexed",
                **doc["data"],
            }]
        }

        r_ingest = api("POST", "/loans/document",
                       base_url, api_key, json=payload)

        if r_ingest.status_code in (200, 201):
            ok(f"Indexed → document_index  "
               f"(doc_id: {doc_id})")
        elif r_ingest.status_code == 404:
            # Try ingest endpoint
            r_ingest2 = api(
                "POST", "/ingest/pdf", base_url, api_key,
                content=pdf_bytes,
                headers={
                    "X-API-Key":        api_key,
                    "Content-Type":     "application/pdf",
                    "X-Applicant-Id":   applicant_id,
                    "X-Application-Id": application_id,
                    "X-Document-Type":  doc["type"],
                    "X-Borrower-Role":  doc["role"],
                }
            )
            if r_ingest2.status_code in (200, 201):
                ok(f"Indexed via /ingest/pdf")
            else:
                warn(f"Ingest: {r_ingest2.status_code} — "
                     "doc stored in S3, index may need manual trigger")
        else:
            warn(f"Ingest returned {r_ingest.status_code}: "
                 f"{r_ingest.text[:60]}")

        # Wait a moment for async assembly
        time.sleep(0.5)

        # Direct Postgres counts — bypasses Redis caches so we can see whether
        # the row actually landed in document_index even when /graph/summary
        # is still serving a stale cached document_count.
        section("Postgres counts (cache-bypass)")
        for table in ("document_index", "income_profiles", "credit_profiles"):
            r_count = api("GET", f"/admin/table-count/{table}",
                          base_url, api_key)
            count = (r_count.json().get("count", "?")
                     if r_count.status_code == 200 else "?")
            print(f"  {table:<22} {str(count):>5} rows")

        # Attribute-index probes — confirm the field landed in the
        # document_index.extracted_fields jsonb where the assemblers
        # and /applicant/{id}/field/{name} can find it.
        probe_fields = {
            "W2_CURRENT":      "box1_wages",
            "CREDIT_REPORT":   "mid_score",
            "APPRAISAL_URAR":  "appraised_value",
        }
        probe_field = probe_fields.get(doc["type"])
        if probe_field:
            r_field = api("GET",
                          f"/applicant/{applicant_id}/field/{probe_field}",
                          base_url, api_key)
            if r_field.status_code == 200:
                body = r_field.json()
                best = body.get("best_value") or {}
                value = best.get("value")
                ok(f"  Indexed field: {probe_field} = {value}")
            else:
                warn(f"  /field/{probe_field}: {r_field.status_code}")

        # Show Redis state after this document
        blank()
        show_redis_state(
            applicant_id, co_id, application_id,
            base_url, api_key,
            f"REDIS AFTER: {doc['label']}"
        )

        if step_mode and doc["step"] < len(DOCUMENTS):
            blank()
            input(f"  {YELLOW}Press ENTER for next document...{RESET}")

    # ── Final report ──────────────────────────────────────────────────────
    banner("FINAL REPORT — FULL PIPELINE STATE")

    # S3 files
    section("S3 Files")
    files = list_files(f"loans/{LOS_ID}/", live, s3)
    for f in files:
        print(f"  {GREEN}✓{RESET}  {f['key']}  ({f['size']:,} bytes)")
    print(f"\n  Total: {len(files)} files")

    # Postgres tables
    section("Postgres Tables")
    tables = [
        "applicants", "applications", "document_index",
        "document_relationships", "income_profiles", "credit_profiles",
    ]
    for table in tables:
        r = api("GET", f"/admin/table-count/{table}",
                base_url, api_key)
        count = (r.json().get("count", "?")
                 if r.status_code == 200 else "?")
        print(f"  {table:<30} {str(count):>5} rows")

    # Final Redis state
    show_redis_state(
        applicant_id, co_id, application_id,
        base_url, api_key,
        "FINAL REDIS STATE"
    )

    # DTI calculation (if context endpoint exists)
    r_ctx = api("GET",
                f"/application/{application_id}/context",
                base_url, api_key)
    if r_ctx.status_code == 200:
        ctx = r_ctx.json()
        section("DTI & LTV Calculation")
        print(f"  Combined qualifying:  "
              f"${ctx.get('combined_qualifying_monthly',0):,.0f}/mo")
        print(f"  Total obligations:    "
              f"${ctx.get('total_monthly_obligations',0):,.0f}/mo")
        if ctx.get("front_end_dti"):
            dti = ctx["front_end_dti"]
            c   = RED if dti > 43 else YELLOW if dti > 36 else GREEN
            print(f"  Front-end DTI:        {c}{dti:.1f}%{RESET}  "
                  f"(PITI / qualifying income)")
        if ctx.get("back_end_dti"):
            dti = ctx["back_end_dti"]
            c   = RED if dti > 50 else YELLOW if dti > 43 else GREEN
            print(f"  Back-end DTI:         {c}{dti:.1f}%{RESET}  "
                  f"(PITI + obligations / income)")
        if ctx.get("ltv"):
            ltv = ctx["ltv"]
            c   = RED if ltv > 95 else YELLOW if ltv > 80 else GREEN
            print(f"  LTV:                  {c}{ltv:.1f}%{RESET}")
        rd = ctx.get("readiness", {})
        section("Readiness Flags")
        flags = [
            ("income_verified",    rd.get("income_verified")),
            ("credit_pulled",      rd.get("credit_pulled")),
            ("appraisal_complete", rd.get("appraisal_complete")),
            ("insurance_bound",    rd.get("insurance_bound")),
            ("flood_cert",         rd.get("flood_cert_received")),
            ("aus_ready",          rd.get("aus_ready")),
        ]
        for name, val in flags:
            icon  = f"{GREEN}✓{RESET}" if val else f"{RED}✗{RESET}"
            print(f"  {icon}  {name}")
        missing = rd.get("missing_items", [])
        if missing:
            print(f"\n  {YELLOW}Still missing:{RESET}")
            for m in missing:
                print(f"    • {m}")
        else:
            print(f"\n  {GREEN}No missing items — loan file complete!{RESET}")

    banner("SIMULATION COMPLETE", "═")
    print(f"  Application ID:  {application_id}")
    print(f"  Primary:         {applicant_id}  (James Okafor)")
    if co_id:
        print(f"  Co-borrower:     {co_id}  (Sarah Okafor)")
    print(f"\n  S3 path:  loans/{LOS_ID}/")
    if not live:
        print(f"  Local:    {LOCAL_S3}/loans/{LOS_ID}/")
    print(f"\n  Dashboard:  {base_url}/dashboard")
    print(f"  Graph:      {base_url}/applicant/{applicant_id}/graph")
    print(f"  Context:    {base_url}/application/{application_id}/context")
    print()

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Drop W2 + credit report + appraisal and watch pipeline")
    parser.add_argument("--live", action="store_true",
                        help="Run against production AWS")
    parser.add_argument("--step", action="store_true",
                        help="Pause between each document drop")
    args = parser.parse_args()

    base_url = PROD_URL      if args.live else LOCAL_URL
    api_key  = PROD_API_KEY  if args.live else LOCAL_API_KEY
    s3       = boto3.client("s3", region_name="us-east-1") \
               if args.live else None

    run(base_url, api_key, args.live, args.step, s3)

if __name__ == "__main__":
    main()
