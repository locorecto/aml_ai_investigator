import json
import logging
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, List

from app.rag.chunking import PolicyChunk, PolicyDocument

logger = logging.getLogger(__name__)


class VectorStore:
    """SQLite-backed vector store for policy chunks."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    title TEXT,
                    source_path TEXT,
                    content_hash TEXT,
                    PRIMARY KEY (doc_id, version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    doc_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    section TEXT,
                    chunk_index INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    citation TEXT,
                    embedding TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id, version)")

    def upsert_chunks(
        self,
        document: PolicyDocument,
        chunks: List[PolicyChunk],
        embeddings: List[List[float]],
    ) -> None:
        if len(chunks) != len(embeddings):
            raise ValueError("Chunk and embedding counts do not match")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO documents (doc_id, version, title, source_path, content_hash)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    document.doc_id,
                    document.version,
                    document.title,
                    str(document.source_path),
                    _hash_text(document.text),
                ),
            )

            for chunk, embedding in zip(chunks, embeddings):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO chunks
                    (chunk_id, doc_id, version, section, chunk_index, text, citation, embedding)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk.chunk_id,
                        chunk.doc_id,
                        chunk.version,
                        chunk.section,
                        chunk.chunk_index,
                        chunk.text,
                        chunk.citation,
                        json.dumps(embedding),
                    ),
                )

        logger.info("Upserted chunks", extra={"doc_id": document.doc_id, "count": len(chunks)})

    def search(self, query_embedding: List[float], top_k: int = 5) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT chunk_id, doc_id, version, section, text, citation, embedding FROM chunks"
            ).fetchall()

        scored: List[Dict] = []
        for row in rows:
            embedding = json.loads(row[6])
            score = _cosine_similarity(query_embedding, embedding)
            scored.append(
                {
                    "chunk_id": row[0],
                    "doc_id": row[1],
                    "version": row[2],
                    "section": row[3],
                    "passage": row[4],
                    "citation": row[5],
                    "score": score,
                }
            )

        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:top_k]


def _hash_text(text: str) -> str:
    return __import__("hashlib").sha256(text.encode("utf-8")).hexdigest()


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(y * y for y in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
