"""PropertyAssembler — assemble a PropertyProfile from available property docs.

Mirrors ``core.income.assembler``: works with partial inputs, never
raises, accumulates warnings, computes a deterministic lineage hash from
the doc-id set so a new doc surfaces as a new profile version downstream.
"""
import hashlib
import logging
from datetime import datetime
from typing import Optional

from core.property.rules import (
    calculate_piti,
    extract_appraisal,
    extract_flood,
    extract_hoa,
    extract_hoi,
    extract_property_tax,
)
from core.property.sources import PITIComponents, PropertyProfile

logger = logging.getLogger(__name__)


class PropertyAssembler:
    def assemble(
        self,
        property_docs: list,
        loan_data: dict,
        property_id: str,
        application_id: str,
    ) -> PropertyProfile:
        warnings: list[str] = []
        profile: dict = {}

        for doc in property_docs or []:
            doc_type = doc.get("document_type", "") or ""

            if "APPRAISAL" in doc_type:
                data = extract_appraisal(doc)
                profile.update(
                    {k: v for k, v in data.items()
                     if k != "warnings" and v is not None}
                )
                warnings.extend(data.get("warnings", []))
            elif "HOI" in doc_type:
                data = extract_hoi(doc)
                profile.update({k: v for k, v in data.items() if v is not None})
            elif "FLOOD" in doc_type:
                data = extract_flood(doc)
                profile.update({k: v for k, v in data.items() if v is not None})
            elif "TAX" in doc_type:
                data = extract_property_tax(doc)
                profile.update({k: v for k, v in data.items() if v is not None})
            elif "HOA" in doc_type:
                data = extract_hoa(doc)
                profile.update({k: v for k, v in data.items() if v is not None})

        if profile.get("flood_insurance_required"):
            warnings.append(
                f"Flood insurance required (zone {profile.get('flood_zone')})"
            )

        piti: Optional[PITIComponents] = None
        if (
            loan_data.get("loan_amount")
            and loan_data.get("interest_rate")
            and profile.get("annual_taxes")
            and profile.get("hoi_monthly")
        ):
            try:
                piti = calculate_piti(
                    loan_amount=float(loan_data["loan_amount"]),
                    interest_rate=float(loan_data.get("interest_rate", 7.0)),
                    loan_term_months=int(loan_data.get("loan_term_months", 360)),
                    annual_taxes=float(profile["annual_taxes"]),
                    hoi_monthly=float(profile["hoi_monthly"]),
                    hoa_monthly=float(profile.get("hoa_monthly", 0) or 0),
                    flood_monthly=float(profile.get("flood_insurance_monthly", 0) or 0),
                )
            except Exception as e:
                warnings.append(f"PITI calculation failed: {e}")
        else:
            missing: list[str] = []
            if not loan_data.get("loan_amount"):
                missing.append("loan_amount")
            if not profile.get("annual_taxes"):
                missing.append("property_tax")
            if not profile.get("hoi_monthly"):
                missing.append("HOI_binder")
            if missing:
                warnings.append(
                    f"PITI incomplete — waiting for: {', '.join(missing)}"
                )

        doc_ids = sorted([d.get("document_id", "") for d in (property_docs or [])])
        lineage_hash = hashlib.sha256(",".join(doc_ids).encode()).hexdigest()[:16]

        requires_review = (
            len(warnings) > 0
            or profile.get("condition_rating") in ("C5", "C6")
            or bool(profile.get("flood_insurance_required"))
        )

        return PropertyProfile(
            property_id=property_id,
            application_id=application_id,
            appraised_value=profile.get("appraised_value"),
            appraisal_date=profile.get("appraisal_date"),
            appraisal_type=profile.get("appraisal_type"),
            appraisal_confidence=profile.get("appraisal_confidence"),
            tax_assessed_value=profile.get("tax_assessed_value"),
            annual_taxes=profile.get("annual_taxes"),
            monthly_taxes=profile.get("monthly_taxes"),
            hoi_annual=profile.get("hoi_annual"),
            hoi_monthly=profile.get("hoi_monthly"),
            flood_zone=profile.get("flood_zone"),
            flood_insurance_required=bool(profile.get("flood_insurance_required", False)),
            flood_insurance_monthly=profile.get("flood_insurance_monthly"),
            hoa_monthly=profile.get("hoa_monthly", 0) or 0,
            condition_rating=profile.get("condition_rating"),
            piti_components=piti,
            assembly_warnings=warnings,
            requires_review=requires_review,
            lineage_hash=lineage_hash,
            assembled_at=datetime.utcnow().isoformat(),
        )
