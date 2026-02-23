# blackroad-notification-hub

Multi-channel notification dispatch with template rendering and delivery tracking.

## Features

- **Four channels** — `email`, `slack`, `webhook`, `push`
- **Template engine** — `{{variable}}` and `{{user.name}}` dotted-path interpolation
- **Delivery log** — every send attempt recorded with latency and success/failure
- **Read/unread management** — mark notifications read, query unread per recipient
- **Batch send** — dispatch a list of notifications in one call
- **Retry failed** — re-attempt all failed deliveries
- **Aggregate stats** — counts by channel and status, delivery success rate

## Quick Start

```python
from src.module import Notification, Channel, send_notification, get_unread

n = Notification.new(
    type="alert",
    recipient="alice@example.com",
    subject="Deploy succeeded",
    body="Version 2.4.1 deployed to production.",
    channel=Channel.EMAIL,
)
send_notification(n)
print(get_unread("alice@example.com"))
```

## CLI

```bash
python src/module.py send alice@example.com "Subject" "Body" --channel email
python src/module.py unread alice@example.com
python src/module.py read <notification-id>
python src/module.py stats --channel email
python src/module.py template-save welcome email "Hi {{name}}" "Welcome, {{name}}!"
python src/module.py template-render welcome '{"name":"Alice"}'
python src/module.py list-templates
python src/module.py retry
```

## Schema

```sql
notifications   -- all notifications with status
templates       -- reusable notification templates
delivery_log    -- per-attempt delivery records
```

## Tests

```bash
pytest tests/ -v
```
