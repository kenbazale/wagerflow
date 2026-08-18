from service.event_topics import get_event_topic


def test_placed_and_rejected_events_use_different_topics():
    assert get_event_topic("placed") == "bet-placed-events"
    assert get_event_topic("rejected") == "bet-rejected-events"
