from pathlib import Path

from app.rag.embeddings import HashEmbeddingProvider
from app.rag.indexer import index_policy_corpus
from app.rag.retriever import retrieve_policy


def test_retrieve_policy_returns_relevant_passage(tmp_path: Path):
    corpus = tmp_path / "policy_corpus"
    corpus.mkdir()
    (corpus / "sar_guidance__v1.md").write_text(
        "# SAR Guidance\nThis policy covers suspicious activity reporting and escalation.\n",
        encoding="utf-8",
    )
    (corpus / "credit_risk__v1.md").write_text(
        "# Credit Risk\nThis policy covers credit card underwriting.\n",
        encoding="utf-8",
    )

    artifacts = tmp_path / "artifacts"
    provider = HashEmbeddingProvider(dimension=64)
    index_policy_corpus(corpus, artifacts, provider)

    case_packet = {
        "case_id": "case-1",
        "party_type": "business",
        "industry": "payments",
        "risk_rating": "high",
        "scenario_codes": ["SAR_ESCALATION"],
        "alerts": [{"trigger_summary": "suspicious activity reporting"}],
    }

    results = retrieve_policy(case_packet, artifacts, provider, top_k=1)
    assert results
    assert results[0]["doc_id"] == "sar_guidance"
