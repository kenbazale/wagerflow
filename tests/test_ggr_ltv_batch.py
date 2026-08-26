from pyspark.sql import Row
from datetime import datetime
from pyspark.sql import Row
from batch.ggr_ltv_batch import build_settled_bets, build_ggr_daily, build_player_ltv


def _ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _bet_placed(spark, rows):
    return spark.createDataFrame(rows, schema="bet_id string, player_id string, market_id string, stake double")


def _settlement(spark, rows):
    return spark.createDataFrame(
        rows,
        schema="bet_id string, outcome string, payout_amount double, settled_at_ts timestamp",
    )

def test_settled_bets_excludes_unsettled_bets(spark):
    bet_placed = _bet_placed(spark, [
        Row(bet_id="b1", player_id="p1", market_id="m1", stake=10.0),
        Row(bet_id="b2", player_id="p1", market_id="m1", stake=20.0),  # never settles
    ])
    settlement = _settlement(spark, [
        Row(bet_id="b1", outcome="WON", payout_amount=15.0, settled_at_ts="2026-08-20 10:00:00"),
    ])

    settled = build_settled_bets(bet_placed, settlement)

    assert settled.count() == 1
    assert settled.collect()[0]["bet_id"] == "b1"


def test_ggr_formula_won_is_stake_minus_payout(spark):
    bet_placed = _bet_placed(spark, [Row(bet_id="b1", player_id="p1", market_id="m1", stake=10.0)])
    settlement = _settlement(spark, [
        Row(bet_id="b1", outcome="WON", payout_amount=25.0, settled_at_ts="2026-08-20 10:00:00"),
    ])

    settled = build_settled_bets(bet_placed, settlement)
    row = settled.collect()[0]

    assert row["ggr_contribution"] == 10.0 - 25.0  # -15.0, house pays out more than stake


def test_ggr_formula_lost_keeps_full_stake(spark):
    bet_placed = _bet_placed(spark, [Row(bet_id="b1", player_id="p1", market_id="m1", stake=10.0)])
    settlement = _settlement(spark, [
        Row(bet_id="b1", outcome="LOST", payout_amount=0.0, settled_at_ts="2026-08-20 10:00:00"),
    ])

    settled = build_settled_bets(bet_placed, settlement)
    row = settled.collect()[0]

    assert row["ggr_contribution"] == 10.0


def test_ggr_formula_void_nets_to_zero(spark):
    bet_placed = _bet_placed(spark, [Row(bet_id="b1", player_id="p1", market_id="m1", stake=10.0)])
    settlement = _settlement(spark, [
        Row(bet_id="b1", outcome="VOID", payout_amount=10.0, settled_at_ts="2026-08-20 10:00:00"),
    ])

    settled = build_settled_bets(bet_placed, settlement)
    row = settled.collect()[0]

    assert row["ggr_contribution"] == 0.0


def test_ggr_daily_groups_by_date_and_market(spark):
    bet_placed = _bet_placed(spark, [
        Row(bet_id="b1", player_id="p1", market_id="m1", stake=10.0),
        Row(bet_id="b2", player_id="p2", market_id="m1", stake=20.0),
        Row(bet_id="b3", player_id="p1", market_id="m2", stake=30.0),
    ])
    settlement = _settlement(spark, [
        Row(bet_id="b1", outcome="LOST", payout_amount=0.0, settled_at_ts="2026-08-20 10:00:00"),
        Row(bet_id="b2", outcome="LOST", payout_amount=0.0, settled_at_ts="2026-08-20 11:00:00"),
        Row(bet_id="b3", outcome="LOST", payout_amount=0.0, settled_at_ts="2026-08-21 09:00:00"),
    ])

    settled = build_settled_bets(bet_placed, settlement)
    daily = build_ggr_daily(settled).collect()

    assert len(daily) == 2  # (2026-08-20, m1) and (2026-08-21, m2)
    row_m1 = next(r for r in daily if r["market_id"] == "m1")
    assert row_m1["total_stake"] == 30.0
    assert row_m1["bet_count"] == 2
    assert row_m1["ggr"] == 30.0


def test_player_ltv_aggregates_across_all_dates(spark):
    bet_placed = _bet_placed(spark, [
        Row(bet_id="b1", player_id="p1", market_id="m1", stake=10.0),
        Row(bet_id="b2", player_id="p1", market_id="m2", stake=20.0),
        Row(bet_id="b3", player_id="p2", market_id="m1", stake=5.0),
    ])
    settlement = _settlement(spark, [
        Row(bet_id="b1", outcome="LOST", payout_amount=0.0, settled_at_ts="2026-08-20 10:00:00"),
        Row(bet_id="b2", outcome="WON", payout_amount=40.0, settled_at_ts="2026-08-21 10:00:00"),
        Row(bet_id="b3", outcome="LOST", payout_amount=0.0, settled_at_ts="2026-08-22 10:00:00"),
    ])

    settled = build_settled_bets(bet_placed, settlement)
    ltv = {r["player_id"]: r for r in build_player_ltv(settled).collect()}

    assert ltv["p1"]["bet_count"] == 2
    assert ltv["p1"]["ltv"] == (10.0 - 0.0) + (20.0 - 40.0)  # -10.0
    assert ltv["p2"]["ltv"] == 5.0