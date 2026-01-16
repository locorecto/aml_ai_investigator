import argparse
import logging
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from app.rag.embeddings import HashEmbeddingProvider
from app.rag.indexer import index_policy_corpus


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index policy corpus into a local vector store.")
    parser.add_argument("--policy-corpus", default="policy_corpus", help="Path to policy corpus.")
    parser.add_argument("--artifacts", default="artifacts/rag_index", help="Output path for index artifacts.")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    corpus_path = REPO_ROOT / args.policy_corpus
    artifacts_path = REPO_ROOT / args.artifacts

    provider = HashEmbeddingProvider()
    index_policy_corpus(corpus_path, artifacts_path, provider)


if __name__ == "__main__":
    main()
