"""GoldenRecord and IdentityXRef pydantic models."""
import hashlib
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from core.aggregation.status import GoldenRecordStatus


class IdentityXRef(BaseModel):
    xref_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    applicant_id: str
    source_system: str
    source_id: str
    match_confidence: float
    match_method: str
    added_at: datetime = Field(default_factory=datetime.utcnow)


class GoldenRecord(BaseModel):
    applicant_id: str
    full_name: str
    first_name: str
    last_name: str
    dob: str
    ssn_hash: str
    ssn_last4: str
    email: Optional[str] = None
    phone: Optional[str] = None
    address_current: Optional[dict] = None
    status: GoldenRecordStatus = GoldenRecordStatus.PLACEHOLDER
    identity_xrefs: list[IdentityXRef] = []
    application_ids: list[str] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @staticmethod
    def hash_ssn(ssn: str) -> str:
        return hashlib.sha256(
            ssn.replace("-", "").replace(" ", "").encode()
        ).hexdigest()

    @staticmethod
    def generate_applicant_id(sequence: int, role: str = "P") -> str:
        return f"APL-{sequence:05d}-{role}"

    def add_xref(
        self,
        source_system: str,
        source_id: str,
        confidence: float,
        method: str,
    ) -> IdentityXRef:
        for existing in self.identity_xrefs:
            if (
                existing.source_system == source_system
                and existing.source_id == source_id
            ):
                self.updated_at = datetime.utcnow()
                return existing
        xref = IdentityXRef(
            applicant_id=self.applicant_id,
            source_system=source_system,
            source_id=source_id,
            match_confidence=confidence,
            match_method=method,
        )
        self.identity_xrefs.append(xref)
        self.updated_at = datetime.utcnow()
        return xref

    def get_source_id(self, source_system: str) -> Optional[str]:
        for xref in self.identity_xrefs:
            if xref.source_system == source_system:
                return xref.source_id
        return None

    def is_ready(self) -> bool:
        return self.status == GoldenRecordStatus.ACTIVE
