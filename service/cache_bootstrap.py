"""
Sends a batch of bet commands covering three cases:
  - normal bets from players with funds and no restrictions
  - player-004, who is self-excluded (seeded that way in init.sql) -> expect REJECTED/SELF_EXCLUDED
  - an oversized stake exceeding the seeded 500.00 balance -> expect REJECTED/INSUFFICIENT_BALANCE

bet-commands is plain JSON (not Avro) -- it's an internal command, never
read by another service or exposed externally, so the overhead of a
registered schema isn't worth it here. BetPlaced/BetRejected/wallet-events,
which ARE cross-service contracts, are Avro. This distinction — internal
commands vs. cross-service events — is a deliberate, worth-defending choice.
"""
import json
import uuid
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

commands = [
    {'player_id': 'player-001', 'market_id': 'match-101', 'selection': 'Team A to win', 'stake': 50.0, 'odds': 2.1},
    {'player_id': 'player-002', 'market_id': 'match-101', 'selection': 'Team B to win', 'stake': 30.0, 'odds': 3.4},
    {'player_id': 'player-003', 'market_id': 'match-102', 'selection': 'Draw', 'stake': 20.0, 'odds': 5.0},
    {'player_id': 'player-004', 'market_id': 'match-101', 'selection': 'Team A to win', 'stake': 25.0, 'odds': 2.1},  # self-excluded -> rejected
    {'player_id': 'player-005', 'market_id': 'match-102', 'selection': 'Team A to win', 'stake': 750.0, 'odds': 1.8},  # exceeds balance -> rejected
    {'player_id': 'player-001', 'market_id': 'match-102', 'selection': 'Draw', 'stake': 40.0, 'odds': 5.0},
]

for cmd in commands:
    cmd['command_id'] = f"cmd-{uuid.uuid4().hex[:12]}"
    producer.produce('bet-commands', key=cmd['player_id'], value=json.dumps(cmd))

producer.flush()
print(f"Sent {len(commands)} bet commands.")
