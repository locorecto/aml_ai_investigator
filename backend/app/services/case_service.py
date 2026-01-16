import logging
from pathlib import Path
from typing import Dict, List, Tuple

import pyarrow.dataset as ds
import pandas as pd

from app.core.config import DataPaths

logger = logging.getLogger(__name__)


class CaseNotFoundError(KeyError):
    """Raised when a case_id is not found."""


class CaseDataAccess:
    """Access case packet datasets stored as parquet."""

    def __init__(self, paths: DataPaths) -> None:
        self.paths = paths

    def _dataset(self, path: Path) -> ds.Dataset:
        if not path.exists():
            raise FileNotFoundError(f"Dataset path does not exist: {path}")
        return ds.dataset(str(path), format="parquet")

    @staticmethod
    def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.where(pd.notnull(frame), None)

    def list_cases(self, limit: int, offset: int) -> Tuple[List[Dict], int]:
        dataset = self._dataset(self.paths.case_packet)
        columns = [
            "case_id",
            "party_id",
            "party_type",
            "party_name",
            "risk_rating",
            "max_risk_score",
            "alerts_count",
            "txn_count_case",
            "amount_total_usd_case",
            "last_txn_ms_utc_case",
            "case_window_start_ms_utc",
            "case_window_end_ms_utc",
        ]
        table = dataset.to_table(columns=columns)
        frame = table.to_pandas()
        frame = self._normalize_frame(frame)
        total = len(frame)
        frame = frame.sort_values(
            by=["max_risk_score", "last_txn_ms_utc_case"],
            ascending=[False, False],
            na_position="last",
        )
        slice_df = frame.iloc[offset: offset + limit]
        logger.info("Listed %s cases (offset=%s, limit=%s)", len(slice_df), offset, limit)
        return slice_df.to_dict(orient="records"), total

    def get_case_packet(self, case_id: str) -> Dict:
        dataset = self._dataset(self.paths.case_packet)
        filter_expr = ds.field("case_id") == case_id
        table = dataset.to_table(filter=filter_expr)
        frame = self._normalize_frame(table.to_pandas())
        if frame.empty:
            raise CaseNotFoundError(case_id)
        row = frame.iloc[0].to_dict()
        return row

    def get_timeline(self, case_id: str) -> List[Dict]:
        dataset = self._dataset(self.paths.tx_timeline_daily)
        filter_expr = ds.field("case_id") == case_id
        table = dataset.to_table(filter=filter_expr)
        frame = self._normalize_frame(table.to_pandas())
        if frame.empty:
            return []
        frame = frame.sort_values(by=["txn_date_utc", "instrument_type"], ascending=[True, True])
        return frame.to_dict(orient="records")
