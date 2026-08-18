"""
Game Engine — simulates match results arriving from an external sports-data
feed. In production this would be a real odds-provider/results integration;
here it's a lightweight simulator so Settlement Service has something
realistic to react to.

Outcomes are randomized but weighted roughly by the odds already used when
test bets were placed (lower odds = more likely to win), so results feel
plausible rather than uniform-random. A small VOID chance is included
deliberately -- this is what exercises Settlement Service's refund path,
not just the win/loss paths, and voids (abandoned matches, etc.) are a real
part of any sportsbook's operational reality worth modeling.

GameResultDeclared.outcome carries either the winning selection string
(matching what a winning BetPlaced.selection would equal), or the literal
string 'VOID' -- there's no separate status field, VOID is just a sentinel
value in the same field. Settlement Service branches on that.
"""
import random
import uuid
from datetime import datetime, timezone

from avro_utils import make_serializer, produce_avro
from confluent_kafka import Producer

game_result_serializer = make_serializer('game_result_declared.avsc')

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# market_id -> (game_id, [(selection, odds), ...]) for the markets actually
# used by place_bet.py's test commands. Odds are inverted to rough implied
# probabilities for weighting -- lower odds = more likely favourite = more
# likely to win.
MARKETS = {
    'match-101': ('game-101', [
        ('Team A to win', 2.1),
        ('Team B to win', 3.4),
        ('Draw', 5.0),
    ]),
    'match-102': ('game-102', [
        ('Team A to win', 1.8),
        ('Team B to win', 4.5),
        ('Draw', 5.0),
    ]),
}

VOID_PROBABILITY = 0.1  # 1 in 10 markets is voided instead of resolved


def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def choose_outcome(selections):
    """Pick a winning selection, weighted by implied probability (1/odds)."""
    weights = [1 / odds for _, odds in selections]
    total = sum(weights)
    normalized = [w / total for w in weights]
    chosen = random.choices(selections, weights=normalized, k=1)[0]
    return chosen[0]  # selection name


def declare_result(market_id, game_id, selections):
    is_void = random.random() < VOID_PROBABILITY
    outcome = 'VOID' if is_void else choose_outcome(selections)

    result_event = {
        'game_id': game_id,
        'market_id': market_id,
        'outcome': outcome,
        'declared_at': now_ms(),
    }

    produce_avro(producer, 'game-events', market_id, result_event, game_result_serializer)
    producer.flush()

    if is_void:
        print(f"[VOID] {market_id} — match abandoned, all bets to be refunded")
    else:
        print(f"[RESULT] {market_id} — winning selection: '{outcome}'")


if __name__ == '__main__':
    print("Game Engine declaring results for open markets...")
    for market_id, (game_id, selections) in MARKETS.items():
        declare_result(market_id, game_id, selections)
    print("All results declared.")