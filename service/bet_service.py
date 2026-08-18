"""
Bet Service — the saga's entry point.

Two background threads maintain local read caches so the main loop never
needs a synchronous cross-service call to validate a bet:
  - player_cache: self_exclusion / kyc_status, kept current by consuming the
    Debezium CDC topic for player_accounts (never queries Postgres directly
    — that would be reaching into another concern's data store).
  - balance_cache: kept current by consuming wallet-events. This is a LOCAL,
    approximate view for fast pre-validation, not the durable source of
    truth — that's Wallet Service's Postgres table. Documented trade-off:
    since bet-commands are keyed by player_id and consumed by a single
    partition/consumer, a given player's commands are processed serially,
    so this in-memory cache can't race against itself for the same player.
    It CAN drift briefly from Wallet Service's view under multi-instance
    scaling — a known limitation, not a bug, and worth calling out as such.

The actual bet placement is atomic: BetPlaced and the wallet DEBIT are
produced within a single Kafka transaction (same pattern as the curriculum's
Module 7 transactional order+payment producer) so a mid-write crash can
never leave a bet recorded without its stake debited, or vice versa.
"""
import json
import time
import uuid
import threading
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import SerializationContext, MessageField

from avro_utils import make_serializer, make_deserializer, produce_avro, produce_transactional
from cache_bootstrap import seed_default_caches
from event_topics import get_event_topic

# ---- Local caches, kept warm by background consumer threads ----
player_cache = {}   # player_id -> {'self_exclusion': bool, 'kyc_status': str}
balance_cache = {}  # player_id -> float
cache_lock = threading.Lock()

seed_default_caches(player_cache, balance_cache)


def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def player_cache_worker():
    # The CDC topic's schema is generated dynamically by Debezium's Avro
    # converter, not one of our own .avsc files -- so we build the
    # deserializer with schema_str=None, which makes it resolve the writer's
    # schema from the schema ID embedded in each message's wire format. This
    # is the correct way to consume a registry-encoded topic you don't own
    # the schema definition for.
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
    cdc_deserializer = AvroDeserializer(registry_client)

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'bet-service-player-cache',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe(['wagerflow.public.player_accounts'])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            if msg.value() is None:
                continue  # tombstone (player deleted) -- nothing to cache
            player = cdc_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            player_id = player['player_id']
            with cache_lock:
                player_cache[player_id] = {
                    'self_exclusion': player['self_exclusion'],
                    'kyc_status': player['kyc_status'],
                }
        except Exception as e:
            print(f"[PLAYER-CACHE] skip malformed message: {e}")


def wallet_cache_worker():
    wallet_deserializer = make_deserializer('wallet_transaction.avsc')
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'bet-service-balance-cache',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe(['wallet-events'])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            txn = wallet_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            delta = txn['amount'] if txn['direction'] == 'CREDIT' else -txn['amount']
            with cache_lock:
                balance_cache[txn['player_id']] = balance_cache.get(txn['player_id'], 0.0) + delta
        except Exception as e:
            print(f"[BALANCE-CACHE] skip malformed message: {e}")


# ---- Main service: transactional bet placement ----

bet_placed_serializer = make_serializer('bet_placed.avsc')
bet_rejected_serializer = make_serializer('bet_rejected.avsc')
wallet_serializer = make_serializer('wallet_transaction.avsc')

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'bet-service-txn-producer-1',
    'enable.idempotence': True,
    'acks': 'all',
})
producer.init_transactions()

command_consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bet-service',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})
command_consumer.subscribe(['bet-commands'])


def reject_bet(cmd, reason):
    rejection = {
        'command_id': cmd['command_id'],
        'player_id': cmd['player_id'],
        'reason': reason,
        'rejected_at': now_ms(),
    }
    topic = get_event_topic('rejected')
    try:
        produce_transactional(producer, topic, cmd['player_id'], rejection, bet_rejected_serializer)
        producer.flush()
        print(f"[REJECTED] {cmd['player_id']} — {reason}")
    except Exception as exc:
        print(f"[REJECTED-ERROR] {cmd['player_id']} — {exc}")


def place_bet(cmd):
    player_id = cmd['player_id']

    with cache_lock:
        profile = player_cache.get(player_id)
        balance = balance_cache.get(player_id, 0.0)

    if profile is None:
        reject_bet(cmd, 'UNKNOWN_PLAYER')
        return
    if profile.get('self_exclusion'):
        reject_bet(cmd, 'SELF_EXCLUDED')
        return
    if balance < cmd['stake']:
        reject_bet(cmd, 'INSUFFICIENT_BALANCE')
        return

    bet_id = f"bet-{uuid.uuid4().hex[:12]}"
    potential_payout = round(cmd['stake'] * cmd['odds'], 2)

    bet_event = {
        'bet_id': bet_id,
        'player_id': player_id,
        'market_id': cmd['market_id'],
        'selection': cmd['selection'],
        'stake': cmd['stake'],
        'odds': cmd['odds'],
        'potential_payout': potential_payout,
        'placed_at': now_ms(),
    }
    wallet_debit = {
        'transaction_id': f"txn-{uuid.uuid4().hex[:12]}",
        'player_id': player_id,
        'direction': 'DEBIT',
        'amount': cmd['stake'],
        'reference_type': 'BET_STAKE',
        'reference_id': bet_id,
        'occurred_at': now_ms(),
    }

    try:
        producer.begin_transaction()
        produce_avro(producer, get_event_topic('placed'), player_id, bet_event, bet_placed_serializer)
        produce_avro(producer, 'wallet-events', player_id, wallet_debit, wallet_serializer)
        producer.commit_transaction()
        producer.flush()
        with cache_lock:
            balance_cache[player_id] = balance_cache.get(player_id, 0.0) - cmd['stake']
        print(f"[PLACED] {bet_id} for {player_id}: stake={cmd['stake']} on '{cmd['selection']}' "
              f"@ {cmd['odds']} (potential payout {potential_payout})")
    except Exception as e:
        producer.abort_transaction()
        print(f"[ABORTED] bet for {player_id}: {e}")


if __name__ == '__main__':
    threading.Thread(target=player_cache_worker, daemon=True).start()
    threading.Thread(target=wallet_cache_worker, daemon=True).start()
    print("Bet Service running... (warming caches for a few seconds)")
    time.sleep(5)

    try:
        while True:
            msg = command_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                cmd = json.loads(msg.value())
                place_bet(cmd)
            except Exception as e:
                print(f"[SKIP - malformed command] error={e}")
            command_consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        command_consumer.close()
