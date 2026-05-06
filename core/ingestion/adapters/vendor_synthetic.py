"""Synthetic vendor responses for testing + the demo /run-vendor-checks
endpoint. Each generator returns a realistic payload that the matching
real adapter parses end-to-end.
"""
from __future__ import annotations

from datetime import datetime


def _du_recommendation(credit_score: int, dti: float, ltv: float) -> str:
    if credit_score >= 740 and dti <= 45 and ltv <= 80:
        return "Approve/Eligible"
    if credit_score >= 680 and dti <= 50:
        return "Approve/Eligible"
    if credit_score < 640:
        return "Refer with Caution"
    return "Refer"


def generate_du_response(
    *,
    credit_score: int,
    dti: float,
    ltv: float,
    loan_type: str = "conventional",
    casefile_id: str | None = None,
) -> str:
    """Fannie Mae DU XML findings string (no namespace, simple shape)."""
    rec = _du_recommendation(credit_score, dti, ltv)
    casefile = casefile_id or f"DU-CF-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    risk_factors = []
    if dti > 45:
        risk_factors.append(f"DTI ratio {dti:.1f}% exceeds 45%")
    if ltv > 95:
        risk_factors.append(f"LTV ratio {ltv:.1f}% exceeds 95%")
    if credit_score < 680:
        risk_factors.append(f"Credit score {credit_score} below 680 threshold")
    risk_xml = "".join(
        f"<RISK_FACTOR><Description>{r}</Description></RISK_FACTOR>"
        for r in risk_factors
    )
    return (
        f"<AUSDATA>"
        f"<RECOMMENDATION>"
        f"<RecommendationDescription>{rec}</RecommendationDescription>"
        f"</RECOMMENDATION>"
        f"<CasefileIdentifier>{casefile}</CasefileIdentifier>"
        f"<ELIGIBLE_PRODUCT><ProductName>{loan_type.title()} Fixed 30-Year</ProductName></ELIGIBLE_PRODUCT>"
        f"{risk_xml}"
        f"</AUSDATA>"
    )


def generate_lp_response(
    *,
    credit_score: int,
    dti: float,
    ltv: float,
    key_data_id: str | None = None,
) -> str:
    """Freddie Mac LP XML findings string."""
    if credit_score >= 720 and dti <= 45:
        rec = "Accept"
    elif credit_score < 640:
        rec = "Ineligible"
    else:
        rec = "Caution"
    key = key_data_id or f"LP-KD-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    return (
        f"<LPADATA>"
        f"<LPARecommendation>{rec}</LPARecommendation>"
        f"<KeyDataIdentifier>{key}</KeyDataIdentifier>"
        f"<RiskClass>{'A1' if rec == 'Accept' else 'A4'}</RiskClass>"
        f"</LPADATA>"
    )


def generate_fraud_response(
    applicant_id: str, risk_level: str = "low"
) -> dict:
    """Socure-format fraud / KYC response."""
    score = {"low": 0.15, "medium": 0.65, "high": 0.92}.get(risk_level, 0.15)
    return {
        "applicant_id": applicant_id,
        "scores": [{"name": "fraud", "score": score}],
        "fraudScore": score,
        "kyc": {"reasonCodes": [] if risk_level == "low" else ["I919"]},
        "documentVerification": {"status": "verified" if risk_level != "high" else "review"},
        "emailRisk":   {"score": score / 2},
        "phoneRisk":   {"score": score / 2},
        "addressRisk": {"score": score / 2},
        "decisionStatus": "PASS" if risk_level == "low" else "REVIEW",
    }


def generate_voe_response(
    employer: str, annual_salary: float, *, status: str = "A"
) -> dict:
    """The Work Number (TWN) format VOE response."""
    return {
        "reportDate": datetime.utcnow().strftime("%Y-%m-%d"),
        "employments": [{
            "employerName":     employer,
            "employmentStatus": status,  # A=active, T=terminated
            "hireDate":         "2018-06-01",
            "originalHireDate": "2018-06-01",
            "positionTitle":    "Senior Engineer",
        }],
        "salaries": [{
            "basePayAnnual": float(annual_salary),
            "annualSalary":  float(annual_salary),
            "payFrequency":  "biweekly",
        }],
    }


def generate_ssn_response(*, verified: bool = True, dob_match: bool = True,
                           name_match: bool = True) -> dict:
    return {
        "verified":         verified,
        "nameMatch":        name_match,
        "dobMatch":         dob_match,
        "deathRecord":      False,
        "verificationDate": datetime.utcnow().isoformat(),
    }


def generate_ofac_response(*, hit: bool = False) -> dict:
    return {
        "hit":       hit,
        "hitCount":  0 if not hit else 1,
        "matches":   [] if not hit else [{"name": "Example Match", "score": 0.92}],
        "checkedAt": datetime.utcnow().isoformat(),
    }


def generate_flood_response(flood_zone: str = "X") -> dict:
    sfha = flood_zone.upper() not in ("X", "X500", "B", "C")
    return {
        "flood_zone":               flood_zone,
        "sfha":                     sfha,
        "flood_insurance_required": sfha,
        "determination_date":       datetime.utcnow().strftime("%Y-%m-%d"),
        "firm_panel":               "06037C1234F",
    }
