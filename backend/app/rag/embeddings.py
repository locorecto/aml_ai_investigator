import math
import re
from abc import ABC, abstractmethod
from hashlib import sha256
from typing import Iterable, List


TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


class EmbeddingProvider(ABC):
    """Provider-agnostic embedding interface."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError


class HashEmbeddingProvider(EmbeddingProvider):
    """Deterministic embedding based on hashed tokens (no external services)."""

    def __init__(self, dimension: int = 128) -> None:
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: List[str]) -> List[List[float]]:
        return [self._embed_text(text) for text in texts]

    def _embed_text(self, text: str) -> List[float]:
        vector = [0.0] * self._dimension
        for token in TOKEN_RE.findall(text.lower()):
            digest = sha256(token.encode("utf-8")).hexdigest()
            bucket = int(digest[:8], 16) % self._dimension
            sign = -1.0 if int(digest[8:9], 16) % 2 == 0 else 1.0
            vector[bucket] += sign
        return _normalize(vector)


def _normalize(vector: List[float]) -> List[float]:
    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0:
        return vector
    return [v / norm for v in vector]
