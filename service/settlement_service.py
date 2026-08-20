"""
Settlement Service — the saga orchestrator that resolves bets once a
market's result is declared.

Unlike Bet Service's atomic DEBIT+BetPlaced (single Kafka transaction),
settlement is modeled as an explicit two-step saga: publish BetSettled,
then publish the wallet CREDIT (for a WIN payout or a VOID refund). This
is deliberate, not an oversight -- a real settlement engine may need to
call out to fraud/AML checks or other cross-service concerns between
"this bet is settled" and "money has actually moved," so pretending it's
atomic would misrepresent how production settlement systems actually
work. If the second step fails after the first has already committed, we
publish to saga-compensations so the mismatch can be reconciled rather
than silently lost -- that's the whole reason the saga pattern exists.

Business rule: VOID refunds the stake (what the player risked), not the
potential_payout (what they could have won). WIN pays potential_payout.
LOSS pays nothing further -- the stake was already debited at placement.

Local cache of open bets, built purely from bet-placed-events (same
"derive only from the log, never seed a default" lesson learned the hard
way in Bet Service's balance_cache bug) -- keyed by market_id so a
GameResultDeclared can look up every bet placed on that market.
"""
import json 
import uuid
import threading
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import SerializationContext, MessageField

from avro_utils import make_serializer, make_deserializer, produce_avro

# market_id -> list of open bet dicts (bet_id, player_id, selection, stake, potential_payout)
open_bets = defaultdict(list)
cache_lock = threading.Lock()
cache_ready = threading.Event()

bet_placed_deserializer = make_deserializer('bet_placed.avsc')
game_result_deserializer = make_deserializer('game_result_declared.avsc')
wallet_serializer = make_serializer('wallet_transaction.avsc')
bet_settled_serializer = make_serializer('bet_settled.avsc')

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def bet_cache_worker():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': f"settlement-service-bet-cache-{uuid.uuid4().hex}",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe(['bet-placed-events'])
    startup_end_offsets = None
    while True:
        msg = consumer.poll(1.0)
        assignments = consumer.assignment()
        if not assignments:
            continue
        if startup_end_offsets is None:
            startup_end_offsets = {
                partition: consumer.get_watermark_offsets(partition, cached=False)[1]
                for partition in assignments
            }
        if msg is not None and not msg.error():
            try:
                bet = bet_placed_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                with cache_lock:
                    open_bets[bet['market_id']].append(bet)
            except Exception as e:
                print(f"[BET-CACHE] skip malformed message: {e}")
            consumer.commit(msg, asynchronous=False)

        positions = consumer.position(assignments)
        if all(
            position.offset >= startup_end_offsets[partition]
            for partition, position in zip(assignments, positions)
        ):
            cache_ready.set()

def publish_wallet_credit(player_id, amount, reference_type, reference_id):
    credit = {
        'transaction_id': f"txn-{uuid.uuid4().hex[:12]}",
        'player_id': player_id,
        'direction': 'CREDIT',
        'amount': amount,
        'reference_type': reference_type,
        'reference_id': reference_id,
        'occurred_at': now_ms(),
    }
    produce_avro(producer, 'wallet-events', player_id, credit, wallet_serializer)


def publish_bet_settled(bet, market_id, outcome, payout_amount):
    settled = {
        'bet_id': bet['bet_id'],
        'player_id': bet['player_id'],
        'market_id': market_id,
        'outcome': outcome,          # 'WON' | 'LOST' | 'VOID'
        'payout_amount': payout_amount,
        'settled_at': now_ms(),
    }
    produce_avro(producer, 'settlement-events', bet['player_id'], settled, bet_settled_serializer)


def publish_compensation(bet, failed_step, error):
    compensation = {
        'bet_id': bet['bet_id'],
        'player_id': bet['player_id'],
        'failed_step': failed_step,
        'error': str(error),
        'flagged_at': now_ms(),
    }
    producer.produce(
        topic='saga-compensations',
        key=bet['player_id'],
        value=json.dumps(compensation),
    )


def settle_bet(bet, market_id, game_outcome):
    if game_outcome == 'VOID':
        outcome, payout, ref_type = 'VOID', bet['stake'], 'BET_VOID_REFUND'
    elif bet['selection'] == game_outcome:
        outcome, payout, ref_type = 'WON', bet['potential_payout'], 'BET_PAYOUT'
    else:
        outcome, payout, ref_type = 'LOST', 0.0, None

    step = 'BetSettled'
    try:
        publish_bet_settled(bet, market_id, outcome, payout)

        if payout > 0:
            step = 'WalletCredit'
            publish_wallet_credit(bet['player_id'], payout, ref_type, bet['bet_id'])

        producer.flush()
        print(f"[SETTLED] {bet['bet_id']} for {bet['player_id']}: {outcome} (payout={payout})")

    except Exception as e:
        print(f"[COMPENSATION] {bet['bet_id']} failed at step '{step}': {e}")
        publish_compensation(bet, step, e)
        producer.flush()


def game_result_worker():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'settlement-service',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe(['game-events'])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            result = game_result_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            market_id = result['market_id']
            outcome = result['outcome']

            with cache_lock:
                bets = list(open_bets.get(market_id, []))
                open_bets.pop(market_id, None)

            if not bets:
                print(f"[SETTLEMENT] no open bets found for {market_id} (result: {outcome})")
            else:
                for bet in bets:
                    settle_bet(bet, market_id, outcome)

        except Exception as e:
            print(f"[SKIP - malformed result] error={e}")
        consumer.commit(msg, asynchronous=False)


if __name__ == '__main__':
    threading.Thread(target=bet_cache_worker, daemon=True).start()
    print("Settlement Service running... (warming bet cache)")
    if not cache_ready.wait(timeout=30):
        raise RuntimeError("Timed out waiting for bet cache to catch up")
    print("[BET-CACHE] startup catch-up complete")
    try:
        game_result_worker()
    except KeyboardInterrupt:
        print("Settlement service is shutting down...")
