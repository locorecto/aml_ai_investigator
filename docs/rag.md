# Policy RAG

## Corpus
Policy documents live in `policy_corpus/` as markdown or text.

## Indexing
Run:
```
python scripts/index_policies.py
```
This:
- chunks documents into stable IDs
- embeds chunks using an embedding provider
- stores them in a SQLite vector store under `artifacts/rag_index/`

## Retrieval
`retrieve_policy(case_packet)` builds a query from the case packet and returns:
- top passages
- doc ids/versions
- short citations

## Storage
Vector store files are stored in `artifacts/rag_index/`.
