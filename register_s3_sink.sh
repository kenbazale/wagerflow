#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
BUCKET="wagerflow-dkb"
REGION="us-east-1"

curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "wagerflow-s3-sink",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics": "wallet-events,settlement-events,bet-placed-events,rg-alerts",
      "s3.bucket.name": "'"${BUCKET}"'",
      "s3.region": "'"${REGION}"'",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
      "flush.size": "50",
      "schema.compatibility": "BACKWARD",
      "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
      "path.format": "'"'"'year'"'"'=YYYY/'"'"'month'"'"'=MM/'"'"'day'"'"'=dd",
      "partition.duration.ms": "86400000",
      "timestamp.extractor": "Record",
      "locale": "en-US",
      "timezone": "UTC",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "rotate.schedule.interval.ms": "60000"
    }
  }' \
  $CONNECT_URL/connectors

echo ""
echo "Connector status:"
curl -s $CONNECT_URL/connectors/wagerflow-s3-sink/status | python3 -m json.tool