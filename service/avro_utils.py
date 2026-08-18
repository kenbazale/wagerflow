"""
Shared Avro serializer/deserializer setup, so every service builds these the
same way instead of repeating boilerplate. Keeps schema file paths and the
SchemaRegistryClient config in one place.
"""
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
SCHEMAS_DIR = os.environ.get('SCHEMAS_DIR', os.path.join(os.path.dirname(__file__), '..', 'schemas'))

_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})


def _load_schema(filename: str) -> str:
    with open(os.path.join(SCHEMAS_DIR, filename)) as f:
        return f.read()


def make_serializer(schema_filename: str) -> AvroSerializer:
    return AvroSerializer(_registry_client, _load_schema(schema_filename))


def make_deserializer(schema_filename: str) -> AvroDeserializer:
    return AvroDeserializer(_registry_client, _load_schema(schema_filename))


def produce_avro(producer, topic, key, value, serializer):
    producer.produce(
        topic=topic,
        key=key,
        value=serializer(value, SerializationContext(topic, MessageField.VALUE)),
    )


def produce_transactional(producer, topic, key, value, serializer):
    producer.begin_transaction()
    try:
        produce_avro(producer, topic, key, value, serializer)
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
        raise
