"""GoldenRecord and IdentityXRef behaviour."""
from core.aggregation.status import GoldenRecordStatus
from core.identity.golden_record import GoldenRecord


def test_hash_ssn_is_deterministic():
    a = GoldenRecord.hash_ssn("123-45-6789")
    b = GoldenRecord.hash_ssn("123 45 6789")
    c = GoldenRecord.hash_ssn("123456789")
    assert a == b == c


def test_generate_applicant_id_pads_zeros():
    assert GoldenRecord.generate_applicant_id(7) == "APL-00007-P"
    assert GoldenRecord.generate_applicant_id(123, "C") == "APL-00123-C"


def test_add_xref_dedupes_same_source_id():
    gr = GoldenRecord(
        applicant_id="APL-00001-P",
        full_name="Alice Adams",
        first_name="Alice",
        last_name="Adams",
        dob="1990-01-01",
        ssn_hash=GoldenRecord.hash_ssn("111-22-3333"),
        ssn_last4="3333",
    )
    gr.add_xref("los", "LOS-1", 1.0, "deterministic")
    gr.add_xref("los", "LOS-1", 1.0, "deterministic")
    assert len(gr.identity_xrefs) == 1


def test_status_helpers():
    gr = GoldenRecord(
        applicant_id="APL-00002-P",
        full_name="Bob Bell",
        first_name="Bob",
        last_name="Bell",
        dob="1990-01-01",
        ssn_hash=GoldenRecord.hash_ssn("123-12-1234"),
        ssn_last4="1234",
    )
    assert not gr.is_ready()
    gr.status = GoldenRecordStatus.ACTIVE
    assert gr.is_ready()
