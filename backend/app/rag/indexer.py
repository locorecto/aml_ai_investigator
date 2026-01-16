import logging
from pathlib import Path
from typing import Iterable, List

from app.rag.chunking import chunk_policy_document, load_policy_documents
from app.rag.embeddings import EmbeddingProvider
from app.storage.vector_store import VectorStore

logger = logging.getLogger(__name__)


def index_policy_corpus(
    corpus_path: Path,
    artifacts_path: Path,
    embedding_provider: EmbeddingProvider,
) -> int:
    """Index policy documents into a local vector store."""
    documents = load_policy_documents(corpus_path)
    if not documents:
        logger.warning("No policy documents found", extra={"path": str(corpus_path)})
        return 0

    artifacts_path.mkdir(parents=True, exist_ok=True)
    store = VectorStore(artifacts_path / "index.sqlite")

    total_chunks = 0
    for doc in documents:
        chunks = chunk_policy_document(doc)
        if not chunks:
            continue
        embeddings = embedding_provider.embed([chunk.text for chunk in chunks])
        store.upsert_chunks(doc, chunks, embeddings)
        total_chunks += len(chunks)

    logger.info(
        "Indexed policy corpus",
        extra={"documents": len(documents), "chunks": total_chunks},
    )
    return total_chunks
