import logging
from pathlib import Path
from typing import Dict, List

from app.rag.embeddings import EmbeddingProvider
from app.storage.vector_store import VectorStore

logger = logging.getLogger(__name__)


def build_query_from_case_packet(case_packet: Dict) -> str:
    """Build a plain-text query from a case packet for retrieval."""
    parts = []
    party_type = case_packet.get("party_type")
    industry = case_packet.get("industry")
    risk_rating = case_packet.get("risk_rating")
    model_types = case_packet.get("model_types") or []
    scenario_codes = case_packet.get("scenario_codes") or []

    if party_type:
        parts.append(f"party_type: {party_type}")
    if industry:
        parts.append(f"industry: {industry}")
    if risk_rating:
        parts.append(f"risk_rating: {risk_rating}")
    if model_types:
        parts.append("model_types: " + ", ".join(model_types))
    if scenario_codes:
        parts.append("scenario_codes: " + ", ".join(scenario_codes))

    alerts = case_packet.get("alerts") or []
    for alert in alerts[:5]:
        summary = alert.get("trigger_summary") or ""
        if summary:
            parts.append(summary)

    return "\n".join(parts).strip()


def retrieve_policy(
    case_packet: Dict,
    artifacts_path: Path,
    embedding_provider: EmbeddingProvider,
    top_k: int = 5,
) -> List[Dict]:
    """Return top policy passages for a given case packet."""
    query = build_query_from_case_packet(case_packet)
    if not query:
        return []

    store = VectorStore(artifacts_path / "index.sqlite")
    query_embedding = embedding_provider.embed([query])[0]
    results = store.search(query_embedding, top_k=top_k)

    logger.info(
        "Retrieved policy passages",
        extra={"case_id": case_packet.get("case_id"), "count": len(results)},
    )
    return results
