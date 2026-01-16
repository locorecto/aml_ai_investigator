import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterable, List, Tuple


SECTION_RE = re.compile(r"^(#{1,6})\s+(.*)$")


@dataclass(frozen=True)
class PolicyDocument:
    doc_id: str
    version: str
    title: str
    text: str
    source_path: Path


@dataclass(frozen=True)
class PolicyChunk:
    chunk_id: str
    doc_id: str
    version: str
    section: str
    chunk_index: int
    text: str
    citation: str


def load_policy_documents(corpus_path: Path) -> List[PolicyDocument]:
    """Load markdown/text documents from the policy corpus directory."""
    if not corpus_path.exists():
        return []

    documents: List[PolicyDocument] = []
    for path in sorted(corpus_path.glob("**/*")):
        if path.is_dir():
            continue
        if path.suffix.lower() not in {".md", ".markdown", ".txt"}:
            continue
        text = path.read_text(encoding="utf-8")
        doc_id, version = _parse_doc_id_and_version(path.stem)
        title = path.stem
        documents.append(PolicyDocument(doc_id=doc_id, version=version, title=title, text=text, source_path=path))
    return documents


def _parse_doc_id_and_version(stem: str) -> Tuple[str, str]:
    if "__v" in stem:
        parts = stem.split("__v", 1)
        return parts[0], f"v{parts[1]}"
    return stem, "v1"


def chunk_policy_document(
    document: PolicyDocument,
    max_chars: int = 1200,
    overlap_chars: int = 100,
) -> List[PolicyChunk]:
    """Chunk a policy document into stable chunks with ids."""
    sections = _split_by_section(document.text)
    chunks: List[PolicyChunk] = []
    for section_title, section_text in sections:
        for chunk_index, chunk_text in enumerate(_chunk_text(section_text, max_chars, overlap_chars)):
            chunk_id = _stable_chunk_id(
                document.doc_id,
                document.version,
                section_title,
                chunk_index,
                chunk_text,
            )
            citation = f"{document.doc_id} {section_title} ({document.version})"
            chunks.append(
                PolicyChunk(
                    chunk_id=chunk_id,
                    doc_id=document.doc_id,
                    version=document.version,
                    section=section_title,
                    chunk_index=chunk_index,
                    text=chunk_text,
                    citation=citation,
                )
            )
    return chunks


def _split_by_section(text: str) -> List[Tuple[str, str]]:
    lines = text.splitlines()
    sections: List[Tuple[str, List[str]]] = []
    current_title = "Introduction"
    current_lines: List[str] = []

    for line in lines:
        match = SECTION_RE.match(line.strip())
        if match:
            if current_lines:
                sections.append((current_title, current_lines))
            current_title = match.group(2).strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_title, current_lines))

    return [(title, "\n".join(lines).strip()) for title, lines in sections if "\n".join(lines).strip()]


def _chunk_text(text: str, max_chars: int, overlap_chars: int) -> Iterable[str]:
    if len(text) <= max_chars:
        yield text
        return

    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        yield text[start:end].strip()
        if end == len(text):
            break
        start = max(0, end - overlap_chars)


def _stable_chunk_id(
    doc_id: str,
    version: str,
    section: str,
    chunk_index: int,
    text: str,
) -> str:
    payload = f"{doc_id}||{version}||{section}||{chunk_index}||{text}"
    return sha256(payload.encode("utf-8")).hexdigest()
