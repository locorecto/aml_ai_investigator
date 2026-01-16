from pathlib import Path

from app.rag.chunking import PolicyDocument, chunk_policy_document


def test_chunk_ids_are_stable():
    doc = PolicyDocument(
        doc_id="policy",
        version="v1",
        title="policy",
        text="# Title\nLine one.\nLine two.\n",
        source_path=Path("policy.md"),
    )
    first = chunk_policy_document(doc)
    second = chunk_policy_document(doc)
    assert [c.chunk_id for c in first] == [c.chunk_id for c in second]
