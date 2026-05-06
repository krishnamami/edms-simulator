"""DocumentReconciler tests using the in-memory FakePostgresStore."""
import pytest

from core.graph.reconciler import DocumentReconciler
from core.graph.models import RelationshipType


def _doc(doc_id, doc_type, applicant_id="APL-00001-P", **fields):
    return {
        "document_id":     doc_id,
        "applicant_id":    applicant_id,
        "document_type":   doc_type,
        "extracted_fields": fields,
    }


@pytest.mark.asyncio
async def test_w2_vs_irs_confirms(postgres_store):
    w2  = _doc("D-W2",  "W2_CURRENT",     box1_wages=92400, employer_name="Accenture LLC", tax_year=2024)
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", wages_tips_compensation=92400, employer_name="Accenture LLC", tax_year=2024)
    await postgres_store.save_document(w2)
    await postgres_store.save_document(irs)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    wage_rels = [r for r in rels if "box1_wages" in (r.field_name or "")]
    assert wage_rels, f"expected a wages relationship, got {[(r.field_name, r.relationship_type) for r in rels]}"
    assert wage_rels[0].relationship_type == RelationshipType.CONFIRMS
    assert wage_rels[0].delta_pct == pytest.approx(0.0, abs=0.1)


@pytest.mark.asyncio
async def test_w2_vs_paystub_confirms_close(postgres_store):
    """W2 box1=92400 vs paystub annualized=92000 → 0.4% delta → confirms."""
    w2      = _doc("D-W2",  "W2_CURRENT",      box1_wages=92400, employer_name="Accenture LLC")
    paystub = _doc("D-PS",  "PAYSTUB_CURRENT", annualized_ytd=92000, employer_name="Accenture LLC")
    await postgres_store.save_document(w2)
    await postgres_store.save_document(paystub)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    wages = [r for r in rels if "box1_wages" in (r.field_name or "") and "annualized_ytd" in (r.field_name or "")]
    assert wages
    assert wages[0].relationship_type == RelationshipType.CONFIRMS
    assert wages[0].delta_pct < 5


@pytest.mark.asyncio
async def test_w2_vs_1099_contradicts(postgres_store):
    w2   = _doc("D-W2",   "W2_CURRENT", box1_wages=92400, employer_name="Accenture LLC")
    nine = _doc("D-1099", "1099_NEC",   amount=45000)
    await postgres_store.save_document(w2)
    await postgres_store.save_document(nine)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    contradicts = [r for r in rels if r.relationship_type == RelationshipType.CONTRADICTS]
    assert contradicts, f"expected CONTRADICTS, got {[(r.field_name, r.relationship_type) for r in rels]}"
    assert contradicts[0].delta_pct > 10


@pytest.mark.asyncio
async def test_employer_name_confirms(postgres_store):
    w2      = _doc("D-W2",  "W2_CURRENT",      box1_wages=92400, employer_name="Accenture LLC")
    paystub = _doc("D-PS",  "PAYSTUB_CURRENT", annualized_ytd=92400, employer_name="Accenture")
    await postgres_store.save_document(w2)
    await postgres_store.save_document(paystub)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    employer = [r for r in rels if "employer_name" in (r.field_name or "")]
    assert employer
    # "Accenture LLC" vs "Accenture" should fuzzy-match > 0.70
    assert employer[0].relationship_type in (
        RelationshipType.CONFIRMS,
        RelationshipType.CORROBORATES,
    )


@pytest.mark.asyncio
async def test_employer_name_contradicts(postgres_store):
    w2      = _doc("D-W2",  "W2_CURRENT",      box1_wages=92400, employer_name="Accenture")
    paystub = _doc("D-PS",  "PAYSTUB_CURRENT", annualized_ytd=92400, employer_name="Dell Technologies")
    await postgres_store.save_document(w2)
    await postgres_store.save_document(paystub)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    employer = [r for r in rels if "employer_name" in (r.field_name or "")]
    assert employer
    assert employer[0].relationship_type == RelationshipType.CONTRADICTS


@pytest.mark.asyncio
async def test_missing_fields_skipped(postgres_store):
    w2  = _doc("D-W2",  "W2_CURRENT",     employer_name="Accenture")  # no box1_wages
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", wages_tips_compensation=92400)
    await postgres_store.save_document(w2)
    await postgres_store.save_document(irs)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    # No wages relationship since w2 has no box1_wages
    assert not [r for r in rels if "box1_wages" in (r.field_name or "")]


@pytest.mark.asyncio
async def test_unknown_doc_type_pair(postgres_store):
    a = _doc("D-FLOOD", "FLOOD_CERT",      flood_zone="X")
    b = _doc("D-TITLE", "TITLE_COMMITMENT", title_holder="James Okafor")
    await postgres_store.save_document(a)
    await postgres_store.save_document(b)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", a)
    assert rels == []
