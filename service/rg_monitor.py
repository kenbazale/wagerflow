"""
RG (Responsible Gambling) Monitor — Faust stream-processing app that watches
player behavior across wallet-events, bet-placed-events, and
settlement-events for patterns worth flagging to compliance, and publishes
RGAlert to rg-alerts.

Faust tables are durable, windowed, changelog-backed state (RocksDB locally,
replicated via a Kafka changelog topic) -- the same "state is derived
entirely from an event log, never seeded with an assumption" principle
that Bet Service's balance_cache bug taught the hard way. Faust just
formalizes that pattern with built-in windowing instead of a hand-rolled
dict + consumer thread.

Three rules, deliberately different in kind:
  - DEPOSIT_VELOCITY: a simple tiered count-over-window on wallet-events.
  - LOSS_CHASING: escalating stake across consecutive losing bets -- a join
    between bet-placed-events (stake) and settlement-events (outcome),
    since settlement-events doesn't carry the original stake.
  - SESSION_LENGTH: rolling time-since-first-activity per player, tracked
    from either a deposit or a bet placement, whichever comes first.

Known limitations (worth stating explicitly, not papering over):
  - LOSS_CHASING alerts on the bet that broke the pattern, after it was
    already placed -- not as a preventative warning before it happens.
    A real-time preventative version would need this check integrated
    into Bet Service's own placement validation, a much bigger
    architectural change (real-time behavioral scoring in the hot path).
  - LOSS_CHASING's "last N bets" has no time bound, so a player could
    take days between bets and still trigger it. Combining it with
    SESSION_LENGTH (only consider bets within the same session) would
    fix this but is a deliberate scope cut for this build.
  - bet_stakes (used to look up a settled bet's original stake) has no
    expiry, so it grows unboundedly over the life of the service --
    harmless at demo scale, worth windowing in a production version.
"""
import uuid
from datetime import datetime, timezone

try:
    import faust
except ImportError:  # pragma: no cover - exercised in minimal test environments
    class _FaustAppConfig:
        def __init__(self):
            self.table_partitions = 1
            self.topic_partitions = 1

    class _FaustApp:
        def __init__(self, *args, **kwargs):
            self.conf = _FaustAppConfig()

        def topic(self, *args, **kwargs):
            return None

        def Table(self, *args, **kwargs):
            return self

        def tumbling(self, *args, **kwargs):
            return self

        def agent(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

        def main(self):
            pass

    class _FaustModule:
        App = _FaustApp

    faust = _FaustModule()

try:
    from avro_utils import make_serializer, produce_avro, make_deserializer
except Exception:  # pragma: no cover - exercised when schema-registry deps are absent
    def make_serializer(*args, **kwargs):
        return None

    def produce_avro(*args, **kwargs):
        return None

    def make_deserializer(*args, **kwargs):
        return lambda value, ctx: value

try:
    from confluent_kafka import Producer as ConfluentProducer
except ImportError:  # pragma: no cover - exercised in minimal test environments
    class ConfluentProducer:
        def __init__(self, *args, **kwargs):
            pass

        def produce(self, *args, **kwargs):
            pass

        def flush(self):
            pass

try:
    from confluent_kafka.serialization import SerializationContext, MessageField
except ImportError:  # pragma: no cover - exercised in minimal test environments
    class SerializationContext:
        def __init__(self, *args, **kwargs):
            pass

    class MessageField:
        VALUE = 'value'

app = faust.App(
    'wagerflow-rg-monitor',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

# Match the source topic partition layout used in this repo. Faust creates
# table changelog topics from this setting; if stale changelog topics exist
# from earlier runs they must be deleted so they can be recreated with the
# correct partition count.
app.conf.table_partitions = 3
app.conf.topic_partitions = 3

# wallet-events, bet-placed-events, and settlement-events are all
# Avro-encoded via Confluent's wire format, which Faust's native
# serializers don't speak directly -- so we consume each as a raw topic
# and decode with confluent-kafka's own deserializer inside the agent,
# reusing the existing schema-registry-backed serde helpers instead of
# duplicating that config into Faust's own schema registry integration.

wallet_deserializer = make_deserializer('wallet_transaction.avsc')
bet_placed_deserializer = make_deserializer('bet_placed.avsc')
bet_settled_deserializer = make_deserializer('bet_settled.avsc')
rg_alert_serializer = make_serializer('rg_alert.avsc')

wallet_events_topic = app.topic('wallet-events', value_type=bytes)
bet_placed_topic = app.topic('bet-placed-events', value_type=bytes)
settlement_events_topic = app.topic('settlement-events', value_type=bytes)

alert_producer = ConfluentProducer({'bootstrap.servers': 'localhost:9092'})


def is_deposit_event(txn):
    return txn.get('direction') == 'CREDIT' and txn.get('reference_type') == 'DEPOSIT'


def now_ms():
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def publish_alert(player_id, rule, severity, details):
    alert = {
        'alert_id': f"alert-{uuid.uuid4().hex[:12]}",
        'player_id': player_id,
        'rule_triggered': rule,
        'severity': severity,
        'details': details,
        'triggered_at': now_ms(),
    }
    produce_avro(alert_producer, 'rg-alerts', player_id, alert, rg_alert_serializer)
    alert_producer.flush()  # ensure the alert is sent before returning
    print(f"[RG-ALERT] {player_id} - {rule} -({severity} ): {details}")


# ============================================================
# Rule 1: DEPOSIT_VELOCITY
# ============================================================

# player_id -> count of deposits in the current window
deposit_counts = app.Table(
    'deposit-velocity-counts',
    default=int,
    partitions=3,
).tumbling(600, expires=1200)  # 10-minute tumbling window, 20-min table expiry

# Tiered thresholds: (count, severity). A player only gets alerted once
# per tier crossed, not on every deposit after the first threshold.
DEPOSIT_VELOCITY_TIERS = [
    (3, 'LOW'),
    (5, 'MEDIUM'),
    (8, 'HIGH'),
]

TIER_ORDER = {'': 0, 'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}

# player_id -> highest severity tier already alerted in the current window
deposit_alerted_tier = app.Table(
    'deposit-velocity-alerted-tier',
    default=str,
    partitions=3,
).tumbling(600, expires=1200)


@app.agent(wallet_events_topic)
async def track_deposits(stream):
    async for raw_value in stream:
        try:
            txn = wallet_deserializer(raw_value, SerializationContext('wallet-events', MessageField.VALUE))
        except Exception as e:
            print(f"[RG-MONITOR] skip malformed wallet event: {e}")
            continue

        # ---- Deposit velocity ----
        if is_deposit_event(txn):
            player_id = txn['player_id']
            deposit_counts[player_id] += 1
            count = deposit_counts[player_id].now()

            qualifying_tier = None
            for threshold, severity in DEPOSIT_VELOCITY_TIERS:
                if count >= threshold:
                    qualifying_tier = severity

            if qualifying_tier is not None:
                already_alerted = deposit_alerted_tier[player_id].now()
                if TIER_ORDER[qualifying_tier] > TIER_ORDER[already_alerted]:
                    deposit_alerted_tier[player_id] = qualifying_tier
                    publish_alert(
                        player_id,
                        'DEPOSIT_VELOCITY',
                        qualifying_tier,
                        f"{count} deposits within the current 10-minute window",
                    )

        # ---- Session length (first-activity tracking, deposit side) ----
        track_session_activity(txn['player_id'])


# ============================================================
# Rule 2: LOSS_CHASING
# ============================================================

# bet_id -> stake, populated from bet-placed-events so the settlement-side
# agent can look up "what did this bet actually stake" once it resolves --
# settlement-events carries payout_amount, not the original stake.
bet_stakes = app.Table('bet-stakes', default=float, partitions=3)

# player_id -> list of recent settled bets, each {'stake': float, 'outcome': str},
# most recent last. Capped to a short rolling window -- loss-chasing is about
# a consecutive-behavior pattern, not a time window (see known limitations).
recent_bets = app.Table('loss-chasing-recent-bets', default=list, partitions=3)

LOSS_CHASING_LOOKBACK = 5          # how many recent settled bets to remember per player
LOSS_CHASING_MIN_STREAK = 2        # 2+ consecutive losses...
LOSS_CHASING_STAKE_INCREASE = 1.0  # ...followed by a stake strictly greater than the previous bet


@app.agent(bet_placed_topic)
async def track_bet_stakes(stream):
    async for raw_value in stream:
        try:
            bet = bet_placed_deserializer(raw_value, SerializationContext('bet-placed-events', MessageField.VALUE))
        except Exception as e:
            print(f"[RG-MONITOR] skip malformed bet-placed event: {e}")
            continue

        bet_stakes[bet['bet_id']] = bet['stake']

        # ---- Session length (first-activity tracking, bet side) ----
        track_session_activity(bet['player_id'])


@app.agent(settlement_events_topic)
async def track_loss_chasing(stream):
    async for raw_value in stream:
        try:
            settled = bet_settled_deserializer(raw_value, SerializationContext('settlement-events', MessageField.VALUE))
        except Exception as e:
            print(f"[RG-MONITOR] skip malformed settlement event: {e}")
            continue

        if settled['outcome'] == 'VOID':
            continue  # a void isn't a win or a loss -- doesn't factor into chasing

        player_id = settled['player_id']
        stake = bet_stakes[settled['bet_id']]

        history = list(recent_bets[player_id])
        already_seen_bet_ids = {b['bet_id'] for b in history}
        if settled['bet_id'] in already_seen_bet_ids:
            continue
        history.append({'stake': stake, 'outcome': settled['outcome'], 'bet_id': settled['bet_id']})
        history = history[-LOSS_CHASING_LOOKBACK:]
        recent_bets[player_id] = history

        if len(history) < LOSS_CHASING_MIN_STREAK + 1:
            continue  # not enough history yet to evaluate a streak + follow-up bet

        streak = history[-(LOSS_CHASING_MIN_STREAK + 1):-1]
        latest = history[-1]

        all_losses = all(b['outcome'] == 'LOST' for b in streak)
        stake_increased = latest['stake'] > streak[-1]['stake'] + LOSS_CHASING_STAKE_INCREASE

        if all_losses and stake_increased:
            publish_alert(
                player_id,
                'LOSS_CHASING',
                'HIGH',
                f"stake increased to {latest['stake']} after {LOSS_CHASING_MIN_STREAK} consecutive losses",
            )


# ============================================================
# Rule 3: SESSION_LENGTH
# ============================================================

# player_id -> timestamp (ms) of first activity in the current session window
session_start = app.Table(
    'session-start-times',
    default=int,
    partitions=3,
).tumbling(7200, expires=10800)  # 2-hour tumbling window, 3-hour table expiry

SESSION_LENGTH_THRESHOLD_MS = 2 * 60 * 60 * 1000  # 2 hours

# player_id -> whether SESSION_LENGTH has already been alerted this window
session_alerted = app.Table(
    'session-length-alerted',
    default=bool,
    partitions=3,
).tumbling(7200, expires=10800)


def track_session_activity(player_id):
    """Called from both the wallet-events and bet-placed-events agents --
    session length is measured from whichever activity (deposit or bet)
    happens first in the window, not from a single event type."""
    current = session_start[player_id].now()
    now = now_ms()

    if current == 0:
        session_start[player_id] = now
        return

    elapsed = now - current
    if elapsed >= SESSION_LENGTH_THRESHOLD_MS and not session_alerted[player_id].now():
        session_alerted[player_id] = True
        hours = elapsed / (60 * 60 * 1000)
        publish_alert(
            player_id,
            'SESSION_LENGTH',
            'MEDIUM',
            f"continuous activity for {hours:.1f} hours",
        )


if __name__ == '__main__':
    app.main()