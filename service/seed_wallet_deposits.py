"""
Seeds a starting deposit for each player. Run once before testing bets —
without this, every player starts at a 0 balance and every bet would fail
insufficient-funds, which isn't useful for demoing the happy path.

These are plain (non-transactional) produces since a deposit isn't paired
with any other atomic write — unlike a bet, which must be atomic with its
wallet debit.
"""
import uuid
import time
from confluent_kafka import Producer
from avro_utils import make_serializer, produce_avro

producer = Producer({'bootstrap.servers': 'localhost:9092'})
wallet_serializer = make_serializer('wallet_transaction.avsc')

players = ['player-001', 'player-002', 'player-003', 'player-004', 'player-005']

for player_id in players:
    txn = {
        'transaction_id': f'dep-{uuid.uuid4().hex[:12]}',
        'player_id': player_id,
        'direction': 'CREDIT',
        'amount': 500.00,
        'reference_type': 'DEPOSIT',
        'reference_id': f'seed-deposit-{player_id}',
        'occurred_at': int(time.time() * 1000),
    }
    produce_avro(producer, 'wallet-events', player_id, txn, wallet_serializer)

producer.flush()
print(f"Seeded {len(players)} players with a 500.00 starting balance.")
