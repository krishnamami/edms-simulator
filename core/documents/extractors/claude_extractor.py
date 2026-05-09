"""Claude Vision fallback extractor for the indexer / pdf_adapter.

When a deterministic ``pymupdf`` / ``income_extractors`` / etc. extractor
returns ``{}`` (or no extractor exists for the doc type at all), the
caller can fall through to ``extract_with_claude`` to render the first
N pages of the PDF as PNG and ask Claude to read structured fields off
the image. Returns ``({}, 0.5)`` on every failure mode (no API key,
flag disabled, network error, parse error) so the AI fallback can
NEVER crash the pipeline.

Two entry points share the same prompt-building helper:

  ``async def extract_with_claude(...)``      — for async callers
                                                 (BatchIndexer)
  ``def extract_with_claude_sync(...)``       — for sync callers
                                                 (pdf_adapter / router)

``ENABLE_AI_EXTRACTION=false`` short-circuits both before any API call,
so a client that doesn't want token cost can disable AI extraction
entirely without touching code.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Kept for backwards compatibility with the Phase B stub callers
# (pdf_adapter imports both names). The class is no longer raised by
# ``extract`` because the function now returns gracefully on any
# failure — but tests / external callers may still catch it.
class ClaudeExtractorUnavailable(RuntimeError):
    """Raised when the Anthropic API key is not configured.

    Tier-3 made the AI fallback always-graceful (never raises), but
    this exception class is retained so ``pdf_adapter`` and any
    downstream code that catches it keeps compiling.
    """


CLAUDE_MODEL_ID = "claude-sonnet-4-6"

_AI_CONFIDENCE = 0.88   # Documented ceiling for AI-extracted fields.
_GRACEFUL_FAILURE = ({}, 0.5)


# Per-doc-type field hints. The model reads these as a checklist of what
# to look for; missing fields don't penalise — Claude returns whatever
# it can read. Add new entries here as new doc types arrive.
_EXPECTED_FIELDS: dict[str, str] = {
    "IRS_TRANSCRIPT":            "agi, wages_salaries, tax_year, filing_status, self_employment_income, schedule_c_net, schedule_e_net",
    "FORM_1040":                 "agi, total_income, taxable_income, tax_year, filing_status, wages_line1, schedule_c_income, schedule_e_income",
    "TAX_RETURN_1040_CURRENT":   "agi, total_income, taxable_income, tax_year, filing_status, wages_line1, schedule_c_income, schedule_e_income",
    "TAX_RETURN_1040_PRIOR":     "agi, total_income, taxable_income, tax_year, filing_status, wages_line1",
    "SCHEDULE_C":                "gross_receipts, total_expenses, net_profit, business_name, tax_year",
    "SCHEDULE_E":                "rental_income_gross, rental_expenses, net_rental_income, property_count, tax_year",
    "FORM_1099_NEC":             "nonemployee_compensation, payer_name, tax_year",
    "1099_NEC":                  "nonemployee_compensation, payer_name, tax_year",
    "K1_SCHEDULE":               "ordinary_income, guaranteed_payments, partnership_name, tax_year",
    "K1_PARTNERSHIP":            "ordinary_income, guaranteed_payments, partnership_name, tax_year",
    "GIFT_LETTER":               "gift_amount, donor_name, donor_relationship, repayment_required",
    "RETIREMENT_ACCOUNT":        "account_type, balance, vested_balance, institution",
    "ASSET_STATEMENT_RETIREMENT": "account_type, balance, vested_balance, institution",
    "BROKERAGE_ACCOUNT":         "total_value, liquid_value, institution, account_type",
    "ASSET_STATEMENT_BROKERAGE": "total_value, liquid_value, institution, account_type",
    "PURCHASE_AGREEMENT":        "purchase_price, earnest_money, closing_date, seller_concessions",
    "URLA_1003":                 "loan_purpose, loan_amount, interest_rate, loan_term_months, property_type, occupancy",
    "RATE_LOCK":                 "locked_rate, lock_expiry, lock_days, points, loan_amount",
    "OFFER_LETTER":              "employer_name, position_title, start_date, base_salary, employment_type",
    "AVM_REPORT":                "avm_value, confidence_score, model_name, effective_date",
    "CREDIT_EXPLANATION":        "explanation_type, creditor, reason, resolved",
    "CREDIT_EXPLANATION_LETTER": "explanation_type, creditor, reason, resolved",
    # UNKNOWN — used by the builder to classify a shared-drive scan that
    # arrived without metadata. We ask Claude to first identify the doc
    # type (so the synthesised UNCLASSIFIED stub can be re-routed) AND
    # to return whatever loan-identifying fields are visible (los_id,
    # borrower name + SSN-last4) so a downstream lookup can stitch the
    # scan back to an existing applicant.
    "UNKNOWN":                   "document_type, los_id, borrower_name, employee_name, employer_name, employee_ssn_last4, dob, property_address, document_date, key_value_pairs",
    # Income — W-2 / paystub
    "W2_CURRENT":                "box1_wages, box2_federal_tax, employer_name, employer_ein, employee_name, employee_ssn_last4, tax_year, state, state_wages, state_tax",
    "W2_PRIOR":                  "box1_wages, box2_federal_tax, employer_name, employer_ein, employee_name, tax_year, state, state_wages",
    "PAYSTUB_CURRENT":           "ytd_gross, pay_period_end, pay_frequency, employer_name, base_salary, net_pay, federal_tax_withheld, hours_worked",
    "PAYSTUB":                   "ytd_gross, pay_period_end, pay_frequency, employer_name, base_salary, net_pay",
    # Credit
    "CREDIT_REPORT":             "mid_score, experian_score, transunion_score, equifax_score, tradeline_count, total_monthly_payments, derogatory_count, collections_count, active_bankruptcy, oldest_tradeline",
    # Bank statements — multi-page so the field hint helps Claude know
    # which numbers to surface (the running ledger easily distracts).
    "BANK_STATEMENT_M1":         "ending_balance, beginning_balance, avg_daily_balance, total_deposits, total_withdrawals, institution, account_type, account_last4, statement_period_start, statement_period_end, nsf_count, largest_deposit",
    "BANK_STATEMENT_M2":         "ending_balance, beginning_balance, avg_daily_balance, total_deposits, total_withdrawals, institution, account_type, statement_period_start, statement_period_end",
    "BANK_STATEMENT_M3":         "ending_balance, beginning_balance, avg_daily_balance, total_deposits, total_withdrawals, institution, account_type",
    # Property — appraisals + tax / HOA / condo addenda
    "APPRAISAL_URAR":            "appraised_value, property_address, property_type, year_built, gla_sqft, lot_size_sqft, bedrooms, bathrooms, condition, quality, comparable_1_price, comparable_2_price, comparable_3_price, effective_date, market_trend",
    "APPRAISAL_URAR_1073":       "appraised_value, property_address, property_type, year_built, gla_sqft, bedrooms, bathrooms, condition, effective_date",
    "APPRAISAL_UPDATE":          "original_value, updated_value, update_date, days_since_original",
    "TITLE_COMMITMENT":          "commitment_number, effective_date, policy_amount, vesting, exceptions_count, tax_lien_clear, judgment_lien_clear",
    "TITLE_INSURANCE":           "policy_number, policy_amount, effective_date, insured_name",
    "HOI_BINDER":                "policy_number, annual_premium, coverage_dwelling, deductible, carrier, effective_date",
    "HOI_BINDER_HO6":            "policy_number, annual_premium, coverage_dwelling, deductible, carrier, effective_date",
    "FLOOD_CERT":                "flood_zone, requires_insurance, firm_panel, determination_date",
    "PROPERTY_TAX_BILL":         "annual_tax, assessed_value, tax_year, property_address",
    # Vendor returns
    "VOE_TWN":                   "employer_name, employment_status, hire_date, income_amount, income_frequency, position, verification_date",
    "DRIVERS_LICENSE":           "dl_number, state, expiry_date, name, dob",
    "SSN_VALIDATION":            "ssn_valid, name_match, dob_match, deceased_indicator",
    "OFAC_CHECK":                "ofac_clear, sdn_match, pep_match, adverse_media",
    "AUS_DU_FINDINGS":           "recommendation, risk_class, casefile_id, conditions_count",
    # Fixed-income source docs
    "SSA_AWARD_LETTER":          "monthly_benefit, effective_date, benefit_type",
    "PENSION_LETTER":            "monthly_benefit, employer_name, retirement_date, benefit_type",
    # Condo / HOA / inspection addenda
    "HOA_CERT":                  "monthly_dues, special_assessments, reserve_balance, litigation_pending",
    "CONDO_QUESTIONNAIRE":       "total_units, owner_occupied_pct, reserve_balance, litigation_pending, insurance_adequate",
    "SURVEY":                    "lot_dimensions, easements, encroachments",
    "WDO_REPORT":                "findings, treatment_required, structural_damage",
    "WELL_SEPTIC_INSPECTION":    "well_flow_rate_gpm, water_quality, septic_condition, septic_type",
    "WIND_HAIL_INSURANCE":       "annual_premium, carrier, deductible, coverage_amount",
    # Other
    "LEASE_AGREEMENT":           "monthly_rent, lease_start, lease_end, tenant_name, property_address",
    "DIVORCE_DECREE":            "decree_date, alimony_amount, alimony_frequency, child_support_amount, division_of_assets",
}


_SYSTEM_PROMPT = (
    "You are a mortgage document field extractor. Extract structured "
    "fields from the document image(s). Return ONLY valid JSON with "
    "field names as keys and extracted values. No explanation, no "
    "markdown fences, no preamble — JSON object only. For monetary "
    "values, return numbers without $ or commas. For dates, use "
    "YYYY-MM-DD format. For booleans, use true/false. If a field is "
    "unclear or missing, omit it rather than guessing."
)

_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def is_available() -> bool:
    """Backwards-compatible alias for the Phase B stub callers."""
    return _ai_enabled() and bool(os.getenv("ANTHROPIC_API_KEY"))


def _ai_enabled() -> bool:
    """Per the ENABLE_AI_EXTRACTION env flag (default ``true``).
    A deployment that doesn't want token cost flips this to ``false``."""
    return os.getenv("ENABLE_AI_EXTRACTION", "true").lower() == "true"


def _max_pages() -> int:
    """Cap on pages sent to Claude — cost control. Default 3."""
    try:
        return max(1, int(os.getenv("AI_EXTRACTION_MAX_PAGES", "3")))
    except (ValueError, TypeError):
        return 3


def _render_pages_to_png(pdf_bytes: bytes, max_pages: int) -> list[str]:
    """Convert the first ``max_pages`` pages of the PDF to base64 PNG.
    Returns ``[]`` on any rendering failure."""
    try:
        import fitz  # lazy — PyMuPDF is a heavy import
    except ImportError:
        return []
    if not pdf_bytes:
        return []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return []
    images: list[str] = []
    try:
        for i in range(min(max_pages, len(doc))):
            try:
                pix = doc[i].get_pixmap(dpi=150)
                images.append(base64.b64encode(pix.tobytes("png")).decode())
            except Exception:
                continue
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return images


def _build_user_content(
    images_b64: list[str], doc_type: str, doc_category: str
) -> list[dict]:
    """Build the user-message content blocks: each image first, then a
    text block listing the doc-type-specific field hints."""
    field_hint = _EXPECTED_FIELDS.get(doc_type, "Extract all visible fields")
    text = (
        f"This is a {doc_type} document"
        f"{f' (category: {doc_category})' if doc_category else ''}.\n"
        f"Extract all relevant fields. Expected fields for {doc_type}: "
        f"{field_hint}."
    )
    blocks: list[dict] = [
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": img,
            },
        }
        for img in images_b64
    ]
    blocks.append({"type": "text", "text": text})
    return blocks


def _parse_json(text: str) -> dict:
    """Strip markdown fences and parse. Returns ``{}`` on parse error."""
    if not text:
        return {}
    cleaned = _FENCE_RE.sub("", text).strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_message_text(message: Any) -> str:
    """Pull the first text block from an Anthropic message response."""
    try:
        blocks = getattr(message, "content", None) or []
        for block in blocks:
            text = getattr(block, "text", None)
            if text:
                return text
            if isinstance(block, dict) and block.get("text"):
                return block["text"]
    except Exception:
        return ""
    return ""


# ---------------------------------------------------------------------------
# Async entry point — the canonical API for async callers.
# ---------------------------------------------------------------------------

async def extract_with_claude(
    pdf_bytes: bytes,
    doc_type: str,
    doc_category: str = "",
) -> tuple[dict, float]:
    """Render the PDF as page images and ask Claude Vision to read
    structured fields. Returns ``(fields_dict, confidence)``.

    Always returns ``({}, 0.5)`` on any failure path — no exceptions
    propagate. Honors ``ENABLE_AI_EXTRACTION`` and respects the
    ``AI_EXTRACTION_MAX_PAGES`` cap.
    """
    if not _ai_enabled():
        return _GRACEFUL_FAILURE
    if not os.getenv("ANTHROPIC_API_KEY"):
        return _GRACEFUL_FAILURE

    images = _render_pages_to_png(pdf_bytes, _max_pages())
    if not images:
        return _GRACEFUL_FAILURE

    try:
        from anthropic import AsyncAnthropic  # lazy import
        client = AsyncAnthropic()
    except Exception as exc:
        logger.warning(
            f"ai_extraction_client_unavailable "
            f"error_type={type(exc).__name__} error={str(exc)[:300]}"
        )
        return _GRACEFUL_FAILURE

    try:
        message = await client.messages.create(
            model=CLAUDE_MODEL_ID,
            max_tokens=2000,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": _build_user_content(
                        images, doc_type, doc_category,
                    ),
                }
            ],
        )
    except Exception as exc:
        # Inline error into the message so the production stdlib
        # formatter surfaces it in CloudWatch (extra={} keys are
        # dropped by the default formatter).
        logger.warning(
            f"ai_extraction_api_failed doc_type={doc_type} "
            f"error_type={type(exc).__name__} error={str(exc)[:300]}"
        )
        return _GRACEFUL_FAILURE

    fields = _parse_json(_extract_message_text(message))
    if not fields:
        logger.warning(
            "ai_extraction_empty_response",
            extra={"doc_type": doc_type, "pages_sent": len(images)},
        )
        return _GRACEFUL_FAILURE

    logger.info(
        "ai_extraction_complete",
        extra={
            "doc_type":         doc_type,
            "fields_extracted": len(fields),
            "pages_sent":       len(images),
            "model":            CLAUDE_MODEL_ID,
        },
    )
    return fields, _AI_CONFIDENCE


# ---------------------------------------------------------------------------
# Sync entry point — for sync callers (pdf_adapter, router). Uses the
# synchronous Anthropic client so it doesn't need an event loop.
# ---------------------------------------------------------------------------

def extract_with_claude_sync(
    pdf_bytes: bytes,
    doc_type: str,
    doc_category: str = "",
) -> tuple[dict, float]:
    """Synchronous twin of ``extract_with_claude`` — same prompt + same
    graceful-fallback contract, just blocks the calling thread on the
    API roundtrip."""
    if not _ai_enabled():
        return _GRACEFUL_FAILURE
    if not os.getenv("ANTHROPIC_API_KEY"):
        return _GRACEFUL_FAILURE

    images = _render_pages_to_png(pdf_bytes, _max_pages())
    if not images:
        return _GRACEFUL_FAILURE

    try:
        from core.ingestion._claude_client import get_client
        client = get_client()
        if client is None:
            return _GRACEFUL_FAILURE
    except Exception as exc:
        logger.warning(
            f"ai_extraction_client_unavailable "
            f"error_type={type(exc).__name__} error={str(exc)[:300]}"
        )
        return _GRACEFUL_FAILURE

    try:
        message = client.messages.create(
            model=CLAUDE_MODEL_ID,
            max_tokens=2000,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": _build_user_content(
                        images, doc_type, doc_category,
                    ),
                }
            ],
        )
    except Exception as exc:
        # Inline error into the message so the production stdlib
        # formatter surfaces it in CloudWatch (extra={} keys are
        # dropped by the default formatter).
        logger.warning(
            f"ai_extraction_api_failed doc_type={doc_type} "
            f"error_type={type(exc).__name__} error={str(exc)[:300]}"
        )
        return _GRACEFUL_FAILURE

    fields = _parse_json(_extract_message_text(message))
    if not fields:
        logger.warning(
            "ai_extraction_empty_response",
            extra={"doc_type": doc_type, "pages_sent": len(images)},
        )
        return _GRACEFUL_FAILURE

    logger.info(
        "ai_extraction_complete",
        extra={
            "doc_type":         doc_type,
            "fields_extracted": len(fields),
            "pages_sent":       len(images),
            "model":            CLAUDE_MODEL_ID,
        },
    )
    return fields, _AI_CONFIDENCE


# ---------------------------------------------------------------------------
# Phase B compatibility shim — old ``extract`` signature still imported
# by pdf_adapter. Now it just delegates to extract_with_claude_sync and
# returns the graceful fallback if AI is disabled, instead of raising
# NotImplementedError.
# ---------------------------------------------------------------------------

def extract(pdf_bytes: bytes, hint: Optional[str] = None) -> tuple[dict, float]:
    """Legacy entry point. Delegates to ``extract_with_claude_sync``
    with the doc-type hint passed through. Kept for the pdf_adapter
    import; new code should call ``extract_with_claude`` directly."""
    return extract_with_claude_sync(pdf_bytes, hint or "UNKNOWN")
