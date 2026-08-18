"""
Wallet Service — the durable read-side balance projection.

This service does NOT write to wallet-events. It only ever consumes them
(from Bet Service, and later Settlement Service) and folds them into a
running balance per player, upserted into Postgres. This is the same
"fold over the event log" idea from the curriculum's Module 8
reconstruct_state.py and Module 12 CQRS projection — just serving a live
balance instead of a one-off script or an order-status dashboard.

Why the balance isn't computed live from a SUM() query over all events:
that would get slower as the ledger grows, and every balance check would
require a full table scan. Maintaining a running total via upsert is the
standard event-sourcing read-model pattern — O(1) reads regardless of
ledger size.
"""
import psycopg2
from datetime import datetime
from confluent_kafka import Consumer

from avro_utils import make_deserializer
from confluent_kafka.serialization import SerializationContext, MessageField

conn = psycopg2.connect(
    host='localhost',
    dbname='wagerflow',
    user='wagerflow',
    password='wagerflow_dev_pw',
    port=5432,
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS wallet_balances (
    player_id VARCHAR(255) PRIMARY KEY,
    balance DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    last_transaction_id VARCHAR(255),
    updated_at TIMESTAMP
)
""")


wallet_deserializer = make_deserializer('wallet_transaction.avsc')

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'wallet-service',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,

})
consumer.subscribe(['wallet-events'])

def apply_transaction(txn):
    delta = txn['amount'] if txn['direction'] == 'CREDIT' else -txn['amount']
    cur.execute(""" 
      INSERT INTO wallet_balances (player_id, balance, last_transaction_id, updated_at)
      VALUES (%s, %s, %s, %s)
      ON CONFLICT (player_id) DO UPDATE
      SET balance = wallet_balances.balance + EXCLUDED.balance,
      last_transaction_id = EXCLUDED.last_transaction_id,
      updated_at = EXCLUDED.updated_at
      RETURNING balance
    """, (txn['player_id'], delta, txn['transaction_id'], datetime.now()))


print("wallet service is running...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            txn = wallet_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            apply_transaction(txn)
            print(f"[WALLET] {txn['direction']} {txn['amount']:.2f} for {txn['player_id']} "
                  f"({txn['reference_type']} / {txn['reference_id']})")
        except Exception as e:
            print(f"[SKIP - malformed message] error={e}")
        consumer.commit(msg, asynchronous=False)
except KeyboardInterrupt:
    print("wallet service is shutting down...")
finally:
    consumer.close()
    conn.close()
    cur.close()