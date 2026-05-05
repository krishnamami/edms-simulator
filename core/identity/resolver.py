"""Identity resolver — three-strategy match: deterministic -> probabilistic -> new."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel

from core.identity.golden_record import GoldenRecord


class MatchMethod(str, Enum):
    DETERMINISTIC = "deterministic"
    PROBABILISTIC = "probabilistic"
    NEW_RECORD = "new_record"


class IdentitySignals(BaseModel):
    los_id: str
    first_name: str
    last_name: str
    dob: str
    ssn_hash: Optional[str] = None
    ssn_last4: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[dict] = None
    role: str = "primary"


class ResolutionResult(BaseModel):
    applicant_id: str
    match_method: MatchMethod
    match_confidence: float
    is_new_record: bool
    golden_record: GoldenRecord
    resolution_notes: str = ""


class IdentityResolver:
    PROBABILISTIC_THRESHOLD = 0.85

    def __init__(self, xref_store, golden_record_store):
        self.xref_store = xref_store
        self.golden_record_store = golden_record_store

    def resolve(self, signals: IdentitySignals) -> ResolutionResult:
        # Strategy 1: deterministic SSN hash match
        if signals.ssn_hash:
            existing = self.xref_store.find_by_ssn_hash(signals.ssn_hash)
            if existing:
                existing.add_xref("los", signals.los_id, 0.99, "deterministic")
                self.golden_record_store.save(existing)
                return ResolutionResult(
                    applicant_id=existing.applicant_id,
                    match_method=MatchMethod.DETERMINISTIC,
                    match_confidence=0.99,
                    is_new_record=False,
                    golden_record=existing,
                    resolution_notes=f"SSN hash match -> {existing.applicant_id}",
                )

        # Strategy 2: probabilistic name + DOB
        candidates = self.xref_store.find_by_name_dob(
            signals.first_name, signals.last_name, signals.dob
        )
        if candidates:
            best = self._best_candidate(candidates, signals)
            if best["score"] >= self.PROBABILISTIC_THRESHOLD:
                gr = best["record"]
                gr.add_xref(
                    "los", signals.los_id, best["score"], "probabilistic"
                )
                self.golden_record_store.save(gr)
                return ResolutionResult(
                    applicant_id=gr.applicant_id,
                    match_method=MatchMethod.PROBABILISTIC,
                    match_confidence=best["score"],
                    is_new_record=False,
                    golden_record=gr,
                    resolution_notes=(
                        f"Probabilistic match score {best['score']:.2f}"
                    ),
                )

        # Strategy 3: new golden record
        gr = self._create_new(signals)
        return ResolutionResult(
            applicant_id=gr.applicant_id,
            match_method=MatchMethod.NEW_RECORD,
            match_confidence=1.0,
            is_new_record=True,
            golden_record=gr,
            resolution_notes="No match. New golden record created.",
        )

    def _create_new(self, signals: IdentitySignals) -> GoldenRecord:
        seq = self.xref_store.next_sequence()
        role_code = "P" if signals.role == "primary" else "C"
        gr = GoldenRecord(
            applicant_id=GoldenRecord.generate_applicant_id(seq, role_code),
            full_name=f"{signals.first_name} {signals.last_name}",
            first_name=signals.first_name,
            last_name=signals.last_name,
            dob=signals.dob,
            ssn_hash=signals.ssn_hash or "",
            ssn_last4=signals.ssn_last4 or "",
            email=signals.email,
            phone=signals.phone,
            address_current=signals.address,
        )
        gr.add_xref("los", signals.los_id, 1.0, "deterministic")
        self.xref_store.save(gr)
        self.golden_record_store.save(gr)
        return gr

    def _best_candidate(self, candidates, signals) -> dict:
        from rapidfuzz import fuzz
        best = {"score": 0.0, "record": None}
        for c in candidates:
            name_score = (
                fuzz.ratio(
                    f"{signals.first_name} {signals.last_name}".lower(),
                    c.full_name.lower(),
                )
                / 100
            )
            score = name_score * 0.5
            if c.dob == signals.dob:
                score += 0.4
            if signals.email and c.email and c.email == signals.email:
                score += 0.1
            if score > best["score"]:
                best = {"score": score, "record": c}
        return best
