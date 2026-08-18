def get_event_topic(event_type: str) -> str:
    topics = {
        'placed': 'bet-placed-events',
        'rejected': 'bet-rejected-events',
    }
    try:
        return topics[event_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported event type: {event_type}") from exc
