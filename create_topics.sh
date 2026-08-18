#!/bin/bash
# Creates all WagerFlow topics with deliberate cleanup policies:
# - wallet-events is NOT compacted, deliberately: it's the event-sourced
#   ledger, and a player's balance is the SUM of every transaction, not just
#   the latest one. Compaction (latest-value-per-key) would silently destroy
#   the audit trail. Full history, infinite retention.
# - player-account-events (CDC, created separately when the Debezium
#   connector registers) IS compacted: it mirrors a single Postgres row per
#   key, so latest-value-per-key is the correct semantic there.
# - Everything else is a plain event stream, default retention.

set -e

BROKER="LOCALHOST:9092"

docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists\
    --topic bet-placed-events --partitions 3 --replication-factor 1 

docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists\
    --topic bet-rejected-events --partitions 3 --replication-factor 1 

docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic wallet-events --partitions 3 --replication-factor 1 \
  --config cleanup.policy=delete --config retention.ms=-1
 
docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic game-events --partitions 3 --replication-factor 1
 
docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic settlement-events --partitions 3 --replication-factor 1
 
docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic saga-compensations --partitions 3 --replication-factor 1
 
docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic rg-alerts --partitions 3 --replication-factor 1

docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic bet-commands --partitions 3 --replication-factor 1
 
echo "Topics created:"
docker exec wagerflow-kafka kafka-topics --bootstrap-server $BROKER --list

