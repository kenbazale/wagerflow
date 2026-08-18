"""
CQRS Read Model — a dedicated projection service that folds four different
event streams (bet-placed-events, settlement-events, wallet-events,
rg-alerts) into two denormalized Postgres tables built purely for reading:

  - bet_history: one row per bet, updated twice (placed, then settled) --
    the full lifecycle of a single bet in one place, instead of requiring
    a join across three different services' internal state to answer
    "what happened with this bet."
  - player_activity_summary: one row per player, incrementally aggregated
    -- total wagered, total won, deposit activity, RG alert history. The
    kind of view a support agent or compliance dashboard would actually
    query, that no single upstream service owns on its own.

This is the CQRS "read side": every table here is fully derivable by
replaying the four source topics from the beginning. Nothing here is a
source of truth -- Bet Service, Wallet Service, and Settlement Service
own that. This service can be safely dropped and rebuilt from scratch at
any time by replaying from offset 0, which is the whole point of the
pattern.
"""
import psycopg2
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField

from avro_utils import make_deserializer

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
CREATE TABLE IF NOT EXISTS bet_history (
    bet_id VARCHAR(255) PRIMARY KEY,
    player_id VARCHAR(255) NOT NULL,
    market_id VARCHAR(255) NOT NULL,
    selection VARCHAR(255) NOT NULL,
    stake DECIMAL(10, 2) NOT NULL,
    odds DECIMAL(10, 2) NOT NULL,
    potential_payout DECIMAL(10, 2) NOT NULL,
    placed_at TIMESTAMP NOT NULL,
    outcome VARCHAR(20),
    payout_amount DECIMAL(10, 2),
    settled_at TIMESTAMP
)
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_bet_history_player ON bet_history(player_id)")

cur.execute("""
CREATE TABLE IF NOT EXISTS player_activity_summary (
    player_id VARCHAR(255) PRIMARY KEY,
    total_bets INT NOT NULL DEFAULT 0,
    total_wagered DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    total_won DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    total_deposits INT NOT NULL DEFAULT 0,
    total_deposited DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    alert_count INT NOT NULL DEFAULT 0,
    highest_alert_severity VARCHAR(10),
    last_active_at TIMESTAMP
)
""")

bet_placed_deserializer = make_deserializer('bet_placed.avsc')
bet_settled_deserializer = make_deserializer('bet_settled.avsc')
wallet_deserializer = make_deserializer('wallet_transaction.avsc')
rg_alert_deserializer = make_deserializer('rg_alert.avsc')

SEVERITY_RANK = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}


def touch_player(player_id, when):
    cur.execute("""
        INSERT INTO player_activity_summary (player_id, last_active_at)
        VALUES (%s, %s)
        ON CONFLICT (player_id) DO UPDATE
        SET last_active_at = GREATEST(player_activity_summary.last_active_at, EXCLUDED.last_active_at)
    """, (player_id, when))


def handle_bet_placed(bet):
    now = datetime.now()
    cur.execute("""
        INSERT INTO bet_history (bet_id, player_id, market_id, selection, stake, odds, potential_payout, placed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (bet_id) DO NOTHING
    """, (bet['bet_id'], bet['player_id'], bet['market_id'], bet['selection'],
          bet['stake'], bet['odds'], bet['potential_payout'], now))

    touch_player(bet['player_id'], now)
    cur.execute("""
        UPDATE player_activity_summary
        SET total_bets = total_bets + 1,
            total_wagered = total_wagered + %s
        WHERE player_id = %s
    """, (bet['stake'], bet['player_id']))
    print(f"[CQRS] bet_history: recorded placement {bet['bet_id']} for {bet['player_id']}")


def handle_bet_settled(settled):
    now = datetime.now()
    cur.execute("""
        UPDATE bet_history
        SET outcome = %s, payout_amount = %s, settled_at = %s
        WHERE bet_id = %s
    """, (settled['outcome'], settled['payout_amount'], now, settled['bet_id']))

    if settled['outcome'] == 'WON':
        touch_player(settled['player_id'], now)
        cur.execute("""
            UPDATE player_activity_summary
            SET total_won = total_won + %s
            WHERE player_id = %s
        """, (settled['payout_amount'], settled['player_id']))
    print(f"[CQRS] bet_history: recorded settlement {settled['bet_id']} ({settled['outcome']})")


def handle_wallet_event(txn):
    if txn.get('reference_type') != 'DEPOSIT':
        return  # only deposits count toward deposit activity; bet debits/payouts are already tracked via bet_history

    now = datetime.now()
    touch_player(txn['player_id'], now)
    cur.execute("""
        UPDATE player_activity_summary
        SET total_deposits = total_deposits + 1,
            total_deposited = total_deposited + %s
        WHERE player_id = %s
    """, (txn['amount'], txn['player_id']))
    print(f"[CQRS] player_activity_summary: recorded deposit for {txn['player_id']}")


def handle_rg_alert(alert):
    now = datetime.now()
    touch_player(alert['player_id'], now)

    cur.execute("SELECT highest_alert_severity FROM player_activity_summary WHERE player_id = %s", (alert['player_id'],))
    row = cur.fetchone()
    current_highest = row[0] if row else None
    new_severity = alert['severity']

    if current_highest is None or SEVERITY_RANK[new_severity] > SEVERITY_RANK[current_highest]:
        cur.execute("""
            UPDATE player_activity_summary
            SET alert_count = alert_count + 1,
                highest_alert_severity = %s
            WHERE player_id = %s
        """, (new_severity, alert['player_id']))
    else:
        cur.execute("""
            UPDATE player_activity_summary
            SET alert_count = alert_count + 1
            WHERE player_id = %s
        """, (alert['player_id'],))
    print(f"[CQRS] player_activity_summary: recorded {alert['rule_triggered']} alert for {alert['player_id']}")


TOPIC_HANDLERS = {
    'bet-placed-events': (bet_placed_deserializer, handle_bet_placed),
    'settlement-events': (bet_settled_deserializer, handle_bet_settled),
    'wallet-events': (wallet_deserializer, handle_wallet_event),
    'rg-alerts': (rg_alert_deserializer, handle_rg_alert),
}

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cqrs-projection',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})
consumer.subscribe(list(TOPIC_HANDLERS.keys()))

print("CQRS Projection Service running...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            deserializer, handler = TOPIC_HANDLERS[msg.topic()]
            event = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            handler(event)
        except Exception as e:
            print(f"[CQRS] skip malformed message on {msg.topic()}: {e}")
        consumer.commit(msg, asynchronous=False)
except KeyboardInterrupt:
    print("CQRS projection shutting down...")
finally:
    consumer.close()
    conn.close()
    cur.close()