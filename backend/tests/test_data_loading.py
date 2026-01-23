"""Data loading pipeline tests."""
import os
from pathlib import Path

import pytest

from app.pipelines.data_loading import build_case_packet_frames, build_spark_session, load_schema, with_amount_usd


def test_load_schema_reads_module(tmp_path: Path):
    schema_path = tmp_path / "schema_test.py"
    schema_path.write_text("SCHEMA = 'dummy'", encoding="utf-8")
    assert load_schema(schema_path) == "dummy"


def test_load_schema_raises_on_missing(tmp_path: Path):
    schema_path = tmp_path / "schema_missing.py"
    schema_path.write_text("VALUE = 1", encoding="utf-8")
    with pytest.raises(RuntimeError):
        load_schema(schema_path)


@pytest.fixture(scope="module")
def spark_session():
    if os.getenv("RUN_SPARK_TESTS") != "1":
        pytest.skip("Set RUN_SPARK_TESTS=1 to enable Spark integration tests.")
    spark = build_spark_session("test", "1", "UTC")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()


def test_with_amount_usd_adds_column(spark_session):
    df = spark_session.createDataFrame(
        [
            {"amount": 10.0, "currency": "USD"},
            {"amount": 10.0, "currency": "EUR"},
        ]
    )
    result = with_amount_usd(df).orderBy("currency").collect()
    amounts = [row["amount_usd"] for row in result]
    assert amounts[0] == pytest.approx(11.0)
    assert amounts[1] == pytest.approx(10.0)


def test_build_case_packet_frames_smoke(spark_session):
    parties_df = spark_session.createDataFrame(
        [
            {
                "party_id": "P1",
                "party_type": "business",
                "party_name": "Test Co",
                "industry": "retail",
                "country": "US",
                "state": "NY",
                "onboarding_date": "2024-01-01",
                "expected_monthly_volume_usd": 1000.0,
                "expected_avg_txn_usd": 100.0,
                "risk_rating": "medium",
            }
        ]
    )
    counterparties_df = spark_session.createDataFrame(
        [{"counterparty_id": "C1", "counterparty_type": "individual", "country": "US"}]
    )
    merchants_df = spark_session.createDataFrame(
        [
            {
                "merchant_id": "M1",
                "merchant_name": "Merchant",
                "merchant_category": "Retail",
                "country": "US",
                "state": "NY",
            }
        ]
    )
    alerts_df = spark_session.createDataFrame(
        [
            {
                "party_id": "P1",
                "window_start_ms_utc": 1000,
                "window_end_ms_utc": 2000,
                "supporting_txn_ids": "T1",
                "model_type": "model",
                "model_version": "v1",
                "scenario_code": "S1",
                "alert_id": "A1",
                "alert_timestamp_ms_utc": 1500,
                "risk_score": 0.8,
                "severity": "high",
                "trigger_summary": "trigger",
                "amount_total_usd": 500.0,
                "txn_count": 1,
                "features_json": "{}",
                "data_quality_flags": None,
            }
        ]
    )
    transactions_df = spark_session.createDataFrame(
        [
            {
                "party_id": "P1",
                "txn_timestamp_ms_utc": 1500,
                "txn_id": "T1",
                "instrument_type": "card",
                "direction": "out",
                "amount": 100.0,
                "currency": "USD",
                "counterparty_id": "C1",
                "merchant_id": "M1",
                "channel": "online",
                "country": "US",
                "state": "NY",
                "is_international": 0,
                "description": "test",
            }
        ]
    )

    case_packet_df, case_packet_json_df, tx_timeline_daily_df = build_case_packet_frames(
        transactions_df=transactions_df,
        parties_df=parties_df,
        counterparties_df=counterparties_df,
        merchants_df=merchants_df,
        alerts_df=alerts_df,
        support_txn_max=10,
        top_counterparties_max=10,
        top_merchants_max=10,
    )

    assert case_packet_df.count() == 1
    assert case_packet_json_df.count() == 1
    assert tx_timeline_daily_df.count() == 1
