from typing import Any, List, Optional

from pydantic import BaseModel, Field


class Alert(BaseModel):
    ts: Optional[int] = None
    alert_id: Optional[str] = None
    model_type: Optional[str] = None
    scenario_code: Optional[str] = None
    risk_score: Optional[float] = None
    severity: Optional[str] = None
    trigger_summary: Optional[str] = None
    supporting_txn_ids: Optional[str] = None
    amount_total_usd: Optional[float] = None
    txn_count: Optional[int] = None
    features_json: Optional[str] = None
    data_quality_flags: Optional[str] = None


class TopCounterparty(BaseModel):
    amount_total_usd: Optional[float] = None
    txn_count: Optional[int] = None
    intl_ratio: Optional[float] = None
    last_txn_ms_utc: Optional[int] = None
    counterparty_id: Optional[str] = None
    counterparty_type: Optional[str] = None
    country: Optional[str] = None


class TopMerchant(BaseModel):
    amount_total_usd: Optional[float] = None
    txn_count: Optional[int] = None
    last_txn_ms_utc: Optional[int] = None
    merchant_id: Optional[str] = None
    merchant_name: Optional[str] = None
    merchant_category: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None


class SupportingTransaction(BaseModel):
    ts: Optional[int] = None
    txn_id: Optional[str] = None
    instrument_type: Optional[str] = None
    direction: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    counterparty_id: Optional[str] = None
    merchant_id: Optional[str] = None
    channel: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    is_international: Optional[int] = None
    description: Optional[str] = None


class CaseSummary(BaseModel):
    case_id: str
    party_id: Optional[str] = None
    party_type: Optional[str] = None
    party_name: Optional[str] = None
    risk_rating: Optional[str] = None
    max_risk_score: Optional[float] = None
    alerts_count: Optional[int] = None
    txn_count_case: Optional[int] = None
    amount_total_usd_case: Optional[float] = None
    last_txn_ms_utc_case: Optional[int] = None
    case_window_start_ms_utc: Optional[int] = None
    case_window_end_ms_utc: Optional[int] = None


class CasePacket(BaseModel):
    case_id: str
    party_id: Optional[str] = None
    party_type: Optional[str] = None
    party_name: Optional[str] = None
    industry: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    onboarding_date: Optional[str] = None
    expected_monthly_volume_usd: Optional[float] = None
    expected_avg_txn_usd: Optional[float] = None
    risk_rating: Optional[str] = None
    case_window_start_ms_utc: Optional[int] = None
    case_window_end_ms_utc: Optional[int] = None
    alerts_count: Optional[int] = None
    alerts_high: Optional[int] = None
    alerts_medium: Optional[int] = None
    alerts_low: Optional[int] = None
    max_risk_score: Optional[float] = None
    median_risk_score: Optional[float] = None
    model_types: Optional[List[str]] = None
    scenario_codes: Optional[List[str]] = None
    severities: Optional[List[str]] = None
    txn_count_case: Optional[int] = None
    amount_total_usd_case: Optional[float] = None
    median_amount_usd_case: Optional[float] = None
    max_amount_usd_case: Optional[float] = None
    first_txn_ms_utc_case: Optional[int] = None
    last_txn_ms_utc_case: Optional[int] = None
    intl_ratio_case: Optional[float] = None
    instrument_types_case: Optional[List[str]] = None
    alerts: Optional[List[Alert]] = None
    top_counterparties: Optional[List[TopCounterparty]] = None
    top_merchants: Optional[List[TopMerchant]] = None
    supporting_transactions: Optional[List[SupportingTransaction]] = None


class TimelineEntry(BaseModel):
    case_id: str
    party_id: Optional[str] = None
    txn_date_utc: Optional[str] = None
    instrument_type: Optional[str] = None
    txn_count: int
    amount_total_usd: Optional[float] = None


class Pagination(BaseModel):
    limit: int = Field(..., ge=1)
    offset: int = Field(..., ge=0)
    total: int = Field(..., ge=0)


class PaginatedCases(BaseModel):
    items: List[CaseSummary]
    pagination: Pagination
