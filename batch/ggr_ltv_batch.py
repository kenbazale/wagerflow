"""
WagerFlow — Nightly GGR / Player LTV batch job (Phase 7 tail-end).

Reads bet-placed-events and settlement-events Avro files from the S3 data
lake (written by the Kafka Connect S3 sink), joins them on bet_id, and
computes:
  - ggr_daily: gross gaming revenue per day (by settlement date)
  - player_ltv: all-time lifetime value per player

Both tables are FULL OVERWRITES on each run (not incremental) — simplest
correct behavior at this data volume, and avoids incremental-state bugs.

GGR / LTV formula: stake - payout_amount.
This works uniformly across outcomes because payout_amount is already
0.0 for LOST, the refunded stake for VOID, and stake * odds for WON
(see bet_settled.avsc doc comment) — so no outcome branching is needed.

KNOWN CAVEAT: This job only includes bets that have actually produced a
settlement event. Open or failed-to-settle bets are intentionally excluded
from GGR/LTV until they resolve.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

S3_BUCKET = "wagerflow-dkb"
BET_PLACED_PATH = f"s3a://{S3_BUCKET}/topics/bet-placed-events/"
SETTLEMENT_PATH = f"s3a://{S3_BUCKET}/topics/settlement-events/"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "wagerflow"
PG_USER = "wagerflow"
PG_PASSWORD = "wagerflow_dev_pw"  # override via --pg-password if changed
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_SCHEMA = "analytics"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("wagerflow-ggr-ltv-batch")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    # hadoop-aws 3.5.0 uses AWS SDK v2 and pulls in
                    # software.amazon.awssdk:bundle transitively — do NOT
                    # add com.amazonaws:aws-java-sdk-bundle (the old v1
                    # coordinate) explicitly, it doesn't exist at that
                    # groupId for this version and Ivy will fail to
                    # resolve it.
                    "org.apache.hadoop:hadoop-aws:3.5.0",
                    "org.apache.spark:spark-avro_2.13:4.2.0",
                    "org.postgresql:postgresql:42.7.4",
                ]
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider",
        )
        # The S3 sink uses the same local AWS profile in Docker Compose.
        .config("spark.hadoop.fs.s3a.aws.credentials.provider.profile", os.environ.get("AWS_PROFILE", "wagerflow"))
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )


def read_events(spark: SparkSession):
    bet_placed = (
        spark.read.format("avro")
        .load(BET_PLACED_PATH)
        .select(
            F.col("bet_id"),
            F.col("player_id"),
            F.col("market_id"),
            F.col("stake").cast("double"),
        )
    )

    settlement = (
        spark.read.format("avro")
        .load(SETTLEMENT_PATH)
        .select(
            F.col("bet_id"),
            F.col("outcome"),
            F.col("payout_amount").cast("double"),
            # settled_at is timestamp-millis (epoch millis as long) —
            # convert to a real timestamp before deriving a date.
            (F.col("settled_at") / 1000).cast("timestamp").alias("settled_at_ts"),
        )
    )

    return bet_placed, settlement


def build_settled_bets(bet_placed, settlement):
    """Inner join: only bets that have actually settled are included.
    Open/unsettled bets are correctly excluded from GGR/LTV until they
    resolve — this is expected, not a bug."""
    return bet_placed.join(settlement, on="bet_id", how="inner").withColumn(
        "ggr_contribution", F.col("stake") - F.col("payout_amount")
    )


def build_ggr_daily(settled_bets):
    return (
        settled_bets.withColumn("settlement_date", F.to_date("settled_at_ts"))
        .groupBy("settlement_date", "market_id")
        .agg(
            F.sum("stake").alias("total_stake"),
            F.sum("payout_amount").alias("total_payout"),
            F.sum("ggr_contribution").alias("ggr"),
            F.count("bet_id").alias("bet_count"),
        )
        .orderBy("settlement_date", "market_id")
    )


def build_player_ltv(settled_bets):
    return (
        settled_bets.groupBy("player_id")
        .agg(
            F.sum("stake").alias("total_stake"),
            F.sum("payout_amount").alias("total_payout"),
            F.sum("ggr_contribution").alias("ltv"),
            F.count("bet_id").alias("bet_count"),
        )
        .orderBy(F.desc("ltv"))
    )


def write_to_postgres(df, table_name: str, pg_password: str):
    (
        df.write.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", f"{PG_SCHEMA}.{table_name}")
        .option("user", PG_USER)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def main():
    pg_password = PG_PASSWORD
    if "--pg-password" in sys.argv:
        pg_password = sys.argv[sys.argv.index("--pg-password") + 1]

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bet_placed, settlement = read_events(spark)

        bet_placed_count = bet_placed.count()
        settlement_count = settlement.count()
        print(f"Read {bet_placed_count} bet-placed events, {settlement_count} settlement events")

        settled_bets = build_settled_bets(bet_placed, settlement)
        settled_count = settled_bets.count()
        print(f"Joined to {settled_count} settled bets (unsettled bets excluded)")

        ggr_daily = build_ggr_daily(settled_bets)
        player_ltv = build_player_ltv(settled_bets)

        print("\n--- ggr_daily preview ---")
        ggr_daily.show(20, truncate=False)

        print("\n--- player_ltv preview ---")
        player_ltv.show(20, truncate=False)

        write_to_postgres(ggr_daily, "ggr_daily", pg_password)
        write_to_postgres(player_ltv, "player_ltv", pg_password)

        print(f"\nWrote {PG_SCHEMA}.ggr_daily and {PG_SCHEMA}.player_ltv to Postgres.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main() 