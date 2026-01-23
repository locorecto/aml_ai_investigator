# Data and Schemas

## Input Data
Source CSVs live in `data/` and include:
- `transactions.csv`
- `parties.csv`
- `counterparties.csv`
- `merchants.csv`
- `alerts_*.csv` (cash, wires, credit_cards, loans, ngi)

## Output Data
Spark outputs (parquet):
- `data/case_packet`
- `data/case_packet_json`
- `data/tx_timeline_daily`

## Schemas
- PySpark schemas: `data/spark-schemas/*.py`
- JSON schemas: `data/schemas/*.schema.json`

Schemas are strict and reflect the exact structure expected by the pipeline and API.
