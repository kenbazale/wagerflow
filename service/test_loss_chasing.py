"""
Deliberately triggers the LOSS_CHASING rule: two consecutive losing bets
for one player, followed by a bet with a larger stake than the previous
one -- the exact pattern rg_monitor.py's track_loss_chasing agent watches
for.

Unlike place_bet.py's realistic mixed test data, this script exists
purely to exercise one behavior path end-to-end, so it uses a dedicated
test market ('match-999') with a fixed, predictable outcome declared by
declare_test_result() below -- rather than depending on game_engine.py's
randomized results to happen to produce two losses in a row for the same
player, which isn't reliably reproducible.

Run order:
  1. Make sure bet_service.py, wallet_service.py, settlement_service.py,
     and rg_monitor.py (via `faust -A rg_monitor worker -l info`) are all
     running.
  2. python test_loss_chasing.py
  3. Watch the rg_monitor worker's log for a LOSS_CHASING alert.
"""
import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from avro_utils import make_serializer, produce_avro

TEST_PLAYER = 'player-001'
TEST_MARKET = 'match-999'
LOSING_SELECTION = 'Team A to win'   # what the test player will bet on...
WINNING_OUTCOME = 'Team B to win'    # ...but this is what the market resolves to, guaranteeing a loss

command_producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Three bets for the same player on the same market:
#   bet 1: stake 10, will LOSE
#   bet 2: stake 15, will LOSE  (2 consecutive losses now on record)
#   bet 3: stake 50, will LOSE  <- the stake increase after 2 losses that
#                                   should trip the alert
commands = [
    {'player_id': TEST_PLAYER, 'market_id': TEST_MARKET, 'selection': LOSING_SELECTION, 'stake': 10.0, 'odds': 2.0},
    {'player_id': TEST_PLAYER, 'market_id': TEST_MARKET, 'selection': LOSING_SELECTION, 'stake': 15.0, 'odds': 2.0},
    {'player_id': TEST_PLAYER, 'market_id': TEST_MARKET, 'selection': LOSING_SELECTION, 'stake': 50.0, 'odds': 2.0},
]


def send_bet_commands():
    for cmd in commands:
        cmd['command_id'] = f"cmd-{uuid.uuid4().hex[:12]}"
        command_producer.produce('bet-commands', key=cmd['player_id'], value=json.dumps(cmd))
    command_producer.flush()
    print(f"Sent {len(commands)} bet commands for {TEST_PLAYER} on {TEST_MARKET}.")


def declare_test_result():
    """Publishes a single GameResultDeclared for match-999, guaranteeing
    all three bets above lose. Run this only after bet_service.py has had
    time to process and place all three bets -- otherwise Settlement
    Service may find no open bets yet for this market."""
    game_result_serializer = make_serializer('game_result_declared.avsc')
    result_event = {
        'game_id': 'game-999',
        'market_id': TEST_MARKET,
        'outcome': WINNING_OUTCOME,
        'declared_at': int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    produce_avro(command_producer, 'game-events', TEST_MARKET, result_event, game_result_serializer)
    command_producer.flush()
    print(f"Declared result for {TEST_MARKET}: '{WINNING_OUTCOME}' wins (all 3 test bets should LOSE).")


if __name__ == '__main__':
    send_bet_commands()
    print("Waiting 5s for Bet Service to process all three bets before declaring the result...")
    time.sleep(5)
    declare_test_result()