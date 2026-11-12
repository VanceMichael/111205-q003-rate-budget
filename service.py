def eligible(queued_at, available_at, deadline=None):
    if available_at < queued_at:
        raise ValueError('availability cannot precede enqueue time')
    return deadline is None or available_at <= deadline
