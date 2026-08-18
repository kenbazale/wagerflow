from service.avro_utils import produce_transactional


class DummyProducer:
    def __init__(self):
        self.calls = []

    def begin_transaction(self):
        self.calls.append(("begin", None))

    def produce(self, **kwargs):
        self.calls.append(("produce", kwargs))

    def commit_transaction(self):
        self.calls.append(("commit", None))

    def abort_transaction(self):
        self.calls.append(("abort", None))


def test_produce_transactional_wraps_single_message_in_one_transaction():
    producer = DummyProducer()
    serializer = lambda value, ctx: value

    produce_transactional(producer, "topic-a", "key-a", {"hello": "world"}, serializer)

    assert producer.calls[0][0] == "begin"
    assert producer.calls[1][0] == "produce"
    assert producer.calls[2][0] == "commit"
