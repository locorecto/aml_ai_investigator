"""Spark pipeline to build case packet datasets from raw CSV inputs.

Outputs:
  - case_packet (parquet)
  - case_packet_json (parquet)
  - tx_timeline_daily (parquet)
"""
import argparse
import importlib.util
import logging
from pathlib import Path
from typing import Dict, Tuple

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

CSV_OPTIONS = {
    "header": "true",
    "mode": "FAILFAST",
    "quote": "\"",
    "escape": "\"",
    "multiLine": "false",
    "ignoreLeadingWhiteSpace": "true",
    "ignoreTrailingWhiteSpace": "true",
}

FX_RATES = {
    "USD": 1.0,
    "EUR": 1.10,
    "GBP": 1.28,
    "CAD": 0.75,
    "MXN": 0.058,
}

DEFAULTS = {
    "app_name": "AML-Investigator-Copilot",
    "shuffle_partitions": "200",
    "timezone": "UTC",
    "support_txn_max": 200,
    "top_counterparties_max": 50,
    "top_merchants_max": 50,
}

logger = logging.getLogger(__name__)


def build_spark_session(app_name: str, shuffle_partitions: str, timezone: str) -> SparkSession:
    """Create a SparkSession with consistent defaults."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.csv.parser.columnPruning.enabled", "true")
        .getOrCreate()
    )


def load_schema(schema_path: Path):
    """Load a PySpark StructType named SCHEMA from a module path."""
    spec = importlib.util.spec_from_file_location(schema_path.stem, schema_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load schema module: {schema_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "SCHEMA"):
        raise RuntimeError(f"SCHEMA not found in: {schema_path}")
    return module.SCHEMA


def load_raw_data(spark: SparkSession, base_path: Path, schemas_path: Path) -> Dict[str, DataFrame]:
    """Read source CSVs using curated PySpark schemas."""
    if not base_path.exists():
        raise FileNotFoundError(f"Base path does not exist: {base_path}")
    if not schemas_path.exists():
        raise FileNotFoundError(f"Schemas path does not exist: {schemas_path}")

    transactions_schema = load_schema(schemas_path / "transactions.py")
    parties_schema = load_schema(schemas_path / "parties.py")
    counterparties_schema = load_schema(schemas_path / "counterparties.py")
    merchants_schema = load_schema(schemas_path / "merchants.py")
    alerts_schema = load_schema(schemas_path / "alerts_cash.py")

    transactions_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .schema(transactions_schema)
        .csv(str(base_path / "transactions.csv"))
    )

    parties_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .schema(parties_schema)
        .csv(str(base_path / "parties.csv"))
    )

    counterparties_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .schema(counterparties_schema)
        .csv(str(base_path / "counterparties.csv"))
    )

    merchants_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .schema(merchants_schema)
        .csv(str(base_path / "merchants.csv"))
    )

    alerts_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .schema(alerts_schema)
        .csv(str(base_path / "alerts_*.csv"))
    )

    logger.info("Loaded input datasets from %s", base_path)

    return {
        "transactions": transactions_df,
        "parties": parties_df,
        "counterparties": counterparties_df,
        "merchants": merchants_df,
        "alerts": alerts_df,
    }


def with_amount_usd(df: DataFrame, amount_col: str = "amount", currency_col: str = "currency") -> DataFrame:
    """Add amount_usd column with fixed FX rates."""
    expr = F.col(amount_col)
    for currency, rate in FX_RATES.items():
        if currency == "USD":
            expr = F.when(F.col(currency_col) == currency, F.col(amount_col))
        else:
            expr = expr.when(F.col(currency_col) == currency, F.col(amount_col) * F.lit(rate))
    expr = expr.otherwise(F.col(amount_col))
    return df.withColumn("amount_usd", expr)


def build_case_packet_frames(
    transactions_df: DataFrame,
    parties_df: DataFrame,
    counterparties_df: DataFrame,
    merchants_df: DataFrame,
    alerts_df: DataFrame,
    support_txn_max: int,
    top_counterparties_max: int,
    top_merchants_max: int,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Transform raw data into case packet datasets."""
    alerts_enriched = (
        alerts_df
        .withColumn("supporting_txn_id_arr", F.split(F.coalesce(F.col("supporting_txn_ids"), F.lit("")), r"\|"))
        .withColumn("supporting_txn_id_arr", F.expr("filter(supporting_txn_id_arr, x -> x is not null and x != '')"))
    )

    case_index = (
        alerts_enriched
        .select(
            "party_id",
            F.col("window_start_ms_utc").alias("case_window_start_ms_utc"),
            F.col("window_end_ms_utc").alias("case_window_end_ms_utc"),
        )
        .dropDuplicates()
        .withColumn(
            "case_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("party_id"),
                    F.col("case_window_start_ms_utc").cast("string"),
                    F.col("case_window_end_ms_utc").cast("string"),
                ),
                256
            )
        )
    )

    case_party = (
        case_index
        .join(
            parties_df.select(
                "party_id", "party_type", "party_name", "industry", "country", "state",
                "onboarding_date", "expected_monthly_volume_usd", "expected_avg_txn_usd", "risk_rating"
            ),
            on="party_id",
            how="left"
        )
    )

    alerts_case = (
        alerts_enriched.alias("a")
        .join(
            case_index.alias("c"),
            on=(
                (F.col("a.party_id") == F.col("c.party_id")) &
                (F.col("a.window_start_ms_utc") == F.col("c.case_window_start_ms_utc")) &
                (F.col("a.window_end_ms_utc") == F.col("c.case_window_end_ms_utc"))
            ),
            how="inner"
        )
        .select(
            F.col("c.case_id").alias("case_id"),
            F.col("a.party_id").alias("party_id"),
            F.col("a.model_type").alias("model_type"),
            F.col("a.model_version").alias("model_version"),
            F.col("a.scenario_code").alias("scenario_code"),
            F.col("a.alert_id").alias("alert_id"),
            F.col("a.alert_timestamp_ms_utc").alias("alert_timestamp_ms_utc"),
            F.col("a.window_start_ms_utc").alias("window_start_ms_utc"),
            F.col("a.window_end_ms_utc").alias("window_end_ms_utc"),
            F.col("a.risk_score").alias("risk_score"),
            F.col("a.severity").alias("severity"),
            F.col("a.trigger_summary").alias("trigger_summary"),
            F.col("a.supporting_txn_ids").alias("supporting_txn_ids"),
            F.col("a.supporting_txn_id_arr").alias("supporting_txn_id_arr"),
            F.col("a.amount_total_usd").alias("amount_total_usd"),
            F.col("a.txn_count").alias("txn_count"),
            F.col("a.features_json").alias("features_json"),
            F.col("a.data_quality_flags").alias("data_quality_flags"),
        )
    )

    alerts_summary = (
        alerts_case
        .groupBy("case_id", "party_id")
        .agg(
            F.count("*").alias("alerts_count"),
            F.max("risk_score").alias("max_risk_score"),
            F.expr("percentile_approx(risk_score, 0.5)").alias("median_risk_score"),
            F.collect_set("model_type").alias("model_types"),
            F.collect_set("scenario_code").alias("scenario_codes"),
            F.collect_set("severity").alias("severities"),
            F.sum(F.when(F.col("severity") == "high", 1).otherwise(0)).alias("alerts_high"),
            F.sum(F.when(F.col("severity") == "medium", 1).otherwise(0)).alias("alerts_medium"),
            F.sum(F.when(F.col("severity") == "low", 1).otherwise(0)).alias("alerts_low"),
        )
    )

    alerts_details = (
        alerts_case
        .withColumn(
            "alert_struct",
            F.struct(
                F.col("alert_timestamp_ms_utc").alias("ts"),
                F.col("alert_id").alias("alert_id"),
                F.col("model_type").alias("model_type"),
                F.col("scenario_code").alias("scenario_code"),
                F.col("risk_score").alias("risk_score"),
                F.col("severity").alias("severity"),
                F.col("trigger_summary").alias("trigger_summary"),
                F.col("supporting_txn_ids").alias("supporting_txn_ids"),
                F.col("amount_total_usd").alias("amount_total_usd"),
                F.col("txn_count").alias("txn_count"),
                F.col("features_json").alias("features_json"),
                F.col("data_quality_flags").alias("data_quality_flags"),
            )
        )
        .groupBy("case_id", "party_id")
        .agg(F.sort_array(F.collect_list("alert_struct"), asc=False).alias("alerts"))
    )

    tx_case = (
        transactions_df.alias("t")
        .join(case_index.alias("c"), on="party_id", how="inner")
        .where(
            (F.col("t.txn_timestamp_ms_utc") >= F.col("c.case_window_start_ms_utc")) &
            (F.col("t.txn_timestamp_ms_utc") <= F.col("c.case_window_end_ms_utc"))
        )
        .select(F.col("c.case_id"), F.col("t.*"))
    )

    tx_case_usd = with_amount_usd(tx_case)

    tx_summary = (
        tx_case_usd
        .groupBy("case_id", "party_id")
        .agg(
            F.count("*").alias("txn_count_case"),
            F.sum("amount_usd").alias("amount_total_usd_case"),
            F.expr("percentile_approx(amount_usd, 0.5)").alias("median_amount_usd_case"),
            F.max("amount_usd").alias("max_amount_usd_case"),
            F.min("txn_timestamp_ms_utc").alias("first_txn_ms_utc_case"),
            F.max("txn_timestamp_ms_utc").alias("last_txn_ms_utc_case"),
            F.avg("is_international").alias("intl_ratio_case"),
            F.collect_set("instrument_type").alias("instrument_types_case"),
        )
    )

    tx_instrument_breakdown = (
        tx_case_usd
        .groupBy("case_id", "party_id", "instrument_type")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount_usd").alias("amount_total_usd"),
        )
    )

    top_counterparties = (
        tx_case_usd
        .groupBy("case_id", "party_id", "counterparty_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount_usd").alias("amount_total_usd"),
            F.avg("is_international").alias("intl_ratio"),
            F.max("txn_timestamp_ms_utc").alias("last_txn_ms_utc"),
        )
    )

    w_cp = Window.partitionBy("case_id", "party_id").orderBy(F.col("amount_total_usd").desc(), F.col("txn_count").desc())
    top_counterparties_ranked = top_counterparties.withColumn("rnk", F.row_number().over(w_cp)).where(F.col("rnk") <= top_counterparties_max)

    top_counterparties_enriched = (
        top_counterparties_ranked
        .join(counterparties_df, on="counterparty_id", how="left")
        .withColumn(
            "cp_struct",
            F.struct(
                F.col("amount_total_usd").alias("amount_total_usd"),
                F.col("txn_count").alias("txn_count"),
                F.col("intl_ratio").alias("intl_ratio"),
                F.col("last_txn_ms_utc").alias("last_txn_ms_utc"),
                F.col("counterparty_id").alias("counterparty_id"),
                F.col("counterparty_type").alias("counterparty_type"),
                F.col("country").alias("country"),
            )
        )
        .groupBy("case_id", "party_id")
        .agg(F.collect_list("cp_struct").alias("top_counterparties"))
    )

    top_merchants = (
        tx_case_usd
        .where(F.col("merchant_id").isNotNull())
        .groupBy("case_id", "party_id", "merchant_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount_usd").alias("amount_total_usd"),
            F.max("txn_timestamp_ms_utc").alias("last_txn_ms_utc"),
        )
    )

    w_m = Window.partitionBy("case_id", "party_id").orderBy(F.col("amount_total_usd").desc(), F.col("txn_count").desc())
    top_merchants_ranked = top_merchants.withColumn("rnk", F.row_number().over(w_m)).where(F.col("rnk") <= top_merchants_max)

    top_merchants_enriched = (
        top_merchants_ranked
        .join(merchants_df, on="merchant_id", how="left")
        .withColumn(
            "m_struct",
            F.struct(
                F.col("amount_total_usd").alias("amount_total_usd"),
                F.col("txn_count").alias("txn_count"),
                F.col("last_txn_ms_utc").alias("last_txn_ms_utc"),
                F.col("merchant_id").alias("merchant_id"),
                F.col("merchant_name").alias("merchant_name"),
                F.col("merchant_category").alias("merchant_category"),
                F.col("country").alias("country"),
                F.col("state").alias("state"),
            )
        )
        .groupBy("case_id", "party_id")
        .agg(F.collect_list("m_struct").alias("top_merchants"))
    )

    supporting_txn_ids = (
        alerts_case
        .select("case_id", "party_id", F.explode_outer("supporting_txn_id_arr").alias("txn_id"))
        .where(F.col("txn_id").isNotNull() & (F.col("txn_id") != ""))
        .dropDuplicates(["case_id", "party_id", "txn_id"])
    )

    supporting_tx = (
        supporting_txn_ids
        .join(
            tx_case.select(
                "case_id", "party_id", "txn_id", "txn_timestamp_ms_utc", "instrument_type", "direction", "amount",
                "currency", "counterparty_id", "merchant_id", "channel", "country", "state", "is_international", "description"
            ),
            on=["case_id", "party_id", "txn_id"],
            how="left"
        )
    )

    w_stx = Window.partitionBy("case_id", "party_id").orderBy(F.col("txn_timestamp_ms_utc").desc())
    supporting_tx_limited = (
        supporting_tx
        .withColumn("rnk", F.row_number().over(w_stx))
        .where(F.col("rnk") <= support_txn_max)
        .drop("rnk")
    )

    supporting_tx_packet = (
        supporting_tx_limited
        .withColumn(
            "txn_struct",
            F.struct(
                F.col("txn_timestamp_ms_utc").alias("ts"),
                "txn_id",
                "instrument_type",
                "direction",
                "amount",
                "currency",
                "counterparty_id",
                "merchant_id",
                "channel",
                "country",
                "state",
                "is_international",
                "description",
            )
        )
        .groupBy("case_id", "party_id")
        .agg(F.sort_array(F.collect_list("txn_struct"), asc=False).alias("supporting_transactions"))
    )

    case_packet_df = (
        case_party
        .join(alerts_summary, on=["case_id", "party_id"], how="left")
        .join(alerts_details, on=["case_id", "party_id"], how="left")
        .join(tx_summary, on=["case_id", "party_id"], how="left")
        .join(top_counterparties_enriched, on=["case_id", "party_id"], how="left")
        .join(top_merchants_enriched, on=["case_id", "party_id"], how="left")
        .join(supporting_tx_packet, on=["case_id", "party_id"], how="left")
        .select(
            "case_id",
            "party_id",
            "party_type",
            "party_name",
            "industry",
            "country",
            "state",
            "onboarding_date",
            "expected_monthly_volume_usd",
            "expected_avg_txn_usd",
            "risk_rating",
            "case_window_start_ms_utc",
            "case_window_end_ms_utc",
            "alerts_count",
            "alerts_high",
            "alerts_medium",
            "alerts_low",
            "max_risk_score",
            "median_risk_score",
            "model_types",
            "scenario_codes",
            "severities",
            "txn_count_case",
            "amount_total_usd_case",
            "median_amount_usd_case",
            "max_amount_usd_case",
            "first_txn_ms_utc_case",
            "last_txn_ms_utc_case",
            "intl_ratio_case",
            "instrument_types_case",
            "alerts",
            "top_counterparties",
            "top_merchants",
            "supporting_transactions",
        )
    )

    tx_timeline_daily_df = (
        tx_case_usd
        .withColumn(
            "txn_date_utc",
            F.from_unixtime(F.col("txn_timestamp_ms_utc") / F.lit(1000), "yyyy-MM-dd")
        )
        .groupBy("case_id", "party_id", "txn_date_utc", "instrument_type")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount_usd").alias("amount_total_usd"),
        )
    )

    case_packet_json_df = (
        case_packet_df
        .select(
            "case_id",
            F.to_json(
                F.struct(
                    F.col("case_id").alias("case_id"),
                    F.struct(
                        F.col("party_id").alias("party_id"),
                        F.col("party_type").alias("party_type"),
                        F.col("party_name").alias("party_name"),
                        F.col("industry").alias("industry"),
                        F.col("country").alias("country"),
                        F.col("state").alias("state"),
                        F.col("onboarding_date").alias("onboarding_date"),
                        F.col("risk_rating").alias("risk_rating"),
                        F.col("expected_monthly_volume_usd").alias("expected_monthly_volume_usd"),
                        F.col("expected_avg_txn_usd").alias("expected_avg_txn_usd"),
                    ).alias("party"),
                    F.struct(
                        F.col("case_window_start_ms_utc").alias("start_ms_utc"),
                        F.col("case_window_end_ms_utc").alias("end_ms_utc"),
                    ).alias("window"),
                    F.struct(
                        F.col("alerts_count").alias("alerts_count"),
                        F.col("alerts_high").alias("alerts_high"),
                        F.col("alerts_medium").alias("alerts_medium"),
                        F.col("alerts_low").alias("alerts_low"),
                        F.col("max_risk_score").alias("max_risk_score"),
                        F.col("median_risk_score").alias("median_risk_score"),
                        F.col("model_types").alias("model_types"),
                        F.col("scenario_codes").alias("scenario_codes"),
                    ).alias("alerts_summary"),
                    F.col("alerts").alias("alerts"),
                    F.struct(
                        F.col("txn_count_case").alias("txn_count_case"),
                        F.col("amount_total_usd_case").alias("amount_total_usd_case"),
                        F.col("median_amount_usd_case").alias("median_amount_usd_case"),
                        F.col("max_amount_usd_case").alias("max_amount_usd_case"),
                        F.col("first_txn_ms_utc_case").alias("first_txn_ms_utc_case"),
                        F.col("last_txn_ms_utc_case").alias("last_txn_ms_utc_case"),
                        F.col("intl_ratio_case").alias("intl_ratio_case"),
                        F.col("instrument_types_case").alias("instrument_types_case"),
                    ).alias("tx_summary"),
                    F.col("top_counterparties").alias("top_counterparties"),
                    F.col("top_merchants").alias("top_merchants"),
                    F.col("supporting_transactions").alias("supporting_transactions"),
                )
            ).alias("case_packet_json"),
        )
    )

    return case_packet_df, case_packet_json_df, tx_timeline_daily_df


def write_outputs(
    case_packet_df: DataFrame,
    case_packet_json_df: DataFrame,
    tx_timeline_daily_df: DataFrame,
    output_base_path: Path,
) -> None:
    """Write parquet outputs to the target base path."""
    output_base_path.mkdir(parents=True, exist_ok=True)
    case_packet_df.write.mode("overwrite").parquet(str(output_base_path / "case_packet"))
    case_packet_json_df.write.mode("overwrite").parquet(str(output_base_path / "case_packet_json"))
    tx_timeline_daily_df.write.mode("overwrite").parquet(str(output_base_path / "tx_timeline_daily"))


def run_job(base_path: Path, schemas_path: Path, output_base_path: Path) -> None:
    """Run the end-to-end job with basic error handling and logging."""
    spark = None
    try:
        logger.info("Starting case packet job")
        spark = build_spark_session(
            DEFAULTS["app_name"],
            DEFAULTS["shuffle_partitions"],
            DEFAULTS["timezone"],
        )
        spark.sparkContext.setLogLevel("WARN")

        data = load_raw_data(spark, base_path, schemas_path)
        case_packet_df, case_packet_json_df, tx_timeline_daily_df = build_case_packet_frames(
            data["transactions"],
            data["parties"],
            data["counterparties"],
            data["merchants"],
            data["alerts"],
            support_txn_max=DEFAULTS["support_txn_max"],
            top_counterparties_max=DEFAULTS["top_counterparties_max"],
            top_merchants_max=DEFAULTS["top_merchants_max"],
        )

        write_outputs(case_packet_df, case_packet_json_df, tx_timeline_daily_df, output_base_path)

        logger.info("Outputs written to %s", output_base_path)
    except Exception:
        logger.exception("Case packet job failed")
        raise
    finally:
        if spark is not None:
            spark.stop()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for batch execution."""
    parser = argparse.ArgumentParser(description="Build case packet datasets for AML investigator.")
    parser.add_argument("--base-path", default="data", help="Input base path containing CSVs.")
    parser.add_argument("--schemas-path", default="data/spark-schemas", help="Path to PySpark schemas.")
    parser.add_argument("--output-base", default=None, help="Output base path for parquet datasets.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for daily job execution."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    base_path = Path(args.base_path)
    schemas_path = Path(args.schemas_path)
    output_base_path = Path(args.output_base) if args.output_base else base_path

    run_job(base_path, schemas_path, output_base_path)


if __name__ == "__main__":
    main()

