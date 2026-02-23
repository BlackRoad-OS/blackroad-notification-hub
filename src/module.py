#!/usr/bin/env python3
"""BlackRoad Notification Hub
==============================
Multi-channel notification dispatch with template rendering,
delivery tracking, and read/unread management.

Channels: email, slack, webhook, push
Storage : SQLite (notifications + templates + delivery_log)
"""

import sqlite3
import time
import uuid
import json
import os
import re
import argparse
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path

DB_PATH = os.environ.get("NOTIFICATION_HUB_DB", str(Path.home() / ".blackroad" / "notification_hub.db"))


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------

class Channel(str, Enum):
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    PUSH = "push"


class NotificationStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    READ = "read"


@dataclass
class Notification:
    """A single notification message."""
    id: str
    type: str
    recipient: str
    subject: str
    body: str
    channel: Channel
    status: NotificationStatus = NotificationStatus.PENDING
    sent_at: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0

    @classmethod
    def new(
        cls,
        type: str,
        recipient: str,
        subject: str,
        body: str,
        channel: Channel,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Notification":
        return cls(
            id=str(uuid.uuid4()),
            type=type,
            recipient=recipient,
            subject=subject,
            body=body,
            channel=channel,
            metadata=metadata or {},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "recipient": self.recipient,
            "subject": self.subject,
            "body": self.body,
            "channel": self.channel.value if isinstance(self.channel, Channel) else self.channel,
            "status": self.status.value if isinstance(self.status, NotificationStatus) else self.status,
            "sent_at": self.sent_at,
            "created_at": self.created_at,
            "metadata": self.metadata,
            "retry_count": self.retry_count,
        }


@dataclass
class DeliveryLog:
    """One delivery attempt record."""
    id: Optional[int]
    notification_id: str
    channel: str
    attempt_at: float
    success: bool
    error_msg: Optional[str] = None
    latency_ms: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "notification_id": self.notification_id,
            "channel": self.channel,
            "attempt_at": self.attempt_at,
            "success": self.success,
            "error_msg": self.error_msg,
            "latency_ms": self.latency_ms,
        }


@dataclass
class Template:
    """A reusable notification template with variable interpolation."""
    name: str
    channel: str
    subject_template: str
    body_template: str
    created_at: float = field(default_factory=time.time)

    def render(self, context: Dict[str, Any]) -> Tuple[str, str]:
        """Return (rendered_subject, rendered_body)."""
        subject = _simple_render(self.subject_template, context)
        body = _simple_render(self.body_template, context)
        return subject, body

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "channel": self.channel,
            "subject_template": self.subject_template,
            "body_template": self.body_template,
            "created_at": self.created_at,
        }


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def _simple_render(template: str, context: Dict[str, Any]) -> str:
    """
    Render a template by replacing {{variable}} placeholders.
    Supports dotted access: {{user.name}} -> context["user"]["name"]
    """
    def replacer(match: re.Match) -> str:
        key = match.group(1).strip()
        parts = key.split(".")
        value: Any = context
        try:
            for part in parts:
                if isinstance(value, dict):
                    value = value[part]
                else:
                    value = getattr(value, part)
            return str(value)
        except (KeyError, AttributeError, TypeError):
            return match.group(0)  # leave placeholder intact if missing

    return re.sub(r"\{\{(.+?)\}\}", replacer, template)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _ensure_dir(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def get_db_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    _ensure_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    """Create all schema objects."""
    _ensure_dir(db_path)
    with get_db_connection(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS notifications (
                id           TEXT PRIMARY KEY,
                type         TEXT NOT NULL,
                recipient    TEXT NOT NULL,
                subject      TEXT NOT NULL,
                body         TEXT NOT NULL,
                channel      TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                sent_at      REAL,
                created_at   REAL NOT NULL,
                metadata     TEXT NOT NULL DEFAULT '{}',
                retry_count  INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS templates (
                name             TEXT PRIMARY KEY,
                channel          TEXT NOT NULL,
                subject_template TEXT NOT NULL,
                body_template    TEXT NOT NULL,
                created_at       REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS delivery_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id TEXT NOT NULL,
                channel         TEXT NOT NULL,
                attempt_at      REAL NOT NULL,
                success         INTEGER NOT NULL,
                error_msg       TEXT,
                latency_ms      REAL,
                FOREIGN KEY (notification_id) REFERENCES notifications(id)
            );

            CREATE INDEX IF NOT EXISTS idx_notif_recipient
                ON notifications(recipient, status);
            CREATE INDEX IF NOT EXISTS idx_notif_channel
                ON notifications(channel, status);
            CREATE INDEX IF NOT EXISTS idx_delivery_notif
                ON delivery_log(notification_id);
        """)


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------

def _row_to_notification(row: sqlite3.Row) -> Notification:
    return Notification(
        id=row["id"],
        type=row["type"],
        recipient=row["recipient"],
        subject=row["subject"],
        body=row["body"],
        channel=Channel(row["channel"]),
        status=NotificationStatus(row["status"]),
        sent_at=row["sent_at"],
        created_at=row["created_at"],
        metadata=json.loads(row["metadata"] or "{}"),
        retry_count=row["retry_count"],
    )


def send_notification(notification: Notification, db_path: str = DB_PATH) -> bool:
    """
    Persist and 'dispatch' a notification.
    In this implementation delivery is simulated (always succeeds for valid channels).
    Returns True on success, False on failure.
    """
    init_db(db_path)
    start = time.time()
    success = True
    error_msg = None

    # Validate channel
    try:
        Channel(notification.channel if isinstance(notification.channel, str) else notification.channel.value)
    except ValueError:
        success = False
        error_msg = f"Unknown channel: {notification.channel}"

    now = time.time()
    latency_ms = (time.time() - start) * 1000

    new_status = NotificationStatus.SENT if success else NotificationStatus.FAILED
    sent_at = now if success else None

    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO notifications
                (id, type, recipient, subject, body, channel, status, sent_at,
                 created_at, metadata, retry_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                notification.id,
                notification.type,
                notification.recipient,
                notification.subject,
                notification.body,
                notification.channel.value if isinstance(notification.channel, Channel) else notification.channel,
                new_status.value,
                sent_at,
                notification.created_at,
                json.dumps(notification.metadata),
                notification.retry_count,
            ),
        )
        conn.execute(
            """
            INSERT INTO delivery_log
                (notification_id, channel, attempt_at, success, error_msg, latency_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                notification.id,
                notification.channel.value if isinstance(notification.channel, Channel) else notification.channel,
                now,
                1 if success else 0,
                error_msg,
                round(latency_ms, 3),
            ),
        )

    notification.status = new_status
    notification.sent_at = sent_at
    return success


def batch_send(notifications: List[Notification], db_path: str = DB_PATH) -> Dict[str, bool]:
    """
    Send multiple notifications in sequence.
    Returns {notification.id: success} mapping.
    """
    return {n.id: send_notification(n, db_path) for n in notifications}


def mark_read(notification_id: str, db_path: str = DB_PATH) -> bool:
    """
    Mark a notification as read.
    Returns True if the notification existed and was updated.
    """
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        res = conn.execute(
            "UPDATE notifications SET status = ? WHERE id = ? AND status != ?",
            (NotificationStatus.READ.value, notification_id, NotificationStatus.PENDING.value),
        )
    return res.rowcount > 0


def get_unread(recipient: str, db_path: str = DB_PATH) -> List[Notification]:
    """Return all unread (sent) notifications for *recipient*, newest first."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM notifications "
            "WHERE recipient = ? AND status = ? "
            "ORDER BY created_at DESC",
            (recipient, NotificationStatus.SENT.value),
        ).fetchall()
    return [_row_to_notification(r) for r in rows]


def notification_stats(channel: Optional[str] = None, db_path: str = DB_PATH) -> Dict[str, Any]:
    """
    Aggregate delivery statistics.
    If *channel* is given, filter to that channel; otherwise report across all channels.
    """
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        where = "WHERE channel = ?" if channel else ""
        params: tuple = (channel,) if channel else ()

        total = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM notifications {where}", params
        ).fetchone()["cnt"]

        by_status = conn.execute(
            f"SELECT status, COUNT(*) AS cnt FROM notifications {where} GROUP BY status", params
        ).fetchall()
        status_map = {r["status"]: r["cnt"] for r in by_status}

        by_channel = conn.execute(
            f"SELECT channel, COUNT(*) AS cnt FROM notifications {where} GROUP BY channel", params
        ).fetchall()
        channel_map = {r["channel"]: r["cnt"] for r in by_channel}

        # Delivery success rate from delivery_log
        dl_where = "WHERE channel = ?" if channel else ""
        dl_params: tuple = (channel,) if channel else ()
        total_attempts = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM delivery_log {dl_where}", dl_params
        ).fetchone()["cnt"]
        successful = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM delivery_log {dl_where + (' AND' if channel else 'WHERE')} success = 1",
            dl_params,
        ).fetchone()["cnt"]

    delivery_rate = round(successful / total_attempts * 100, 2) if total_attempts else 0.0

    return {
        "total_notifications": total,
        "by_status": status_map,
        "by_channel": channel_map,
        "total_delivery_attempts": total_attempts,
        "successful_deliveries": successful,
        "delivery_success_rate_pct": delivery_rate,
        "filter_channel": channel,
    }


def template_render(template_name: str, context: Dict[str, Any], db_path: str = DB_PATH) -> Dict[str, str]:
    """
    Render a stored template against *context*.
    Returns {"subject": ..., "body": ...}.
    Raises KeyError if the template does not exist.
    """
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM templates WHERE name = ?", (template_name,)
        ).fetchone()
    if row is None:
        raise KeyError(f"Template not found: {template_name!r}")
    tmpl = Template(
        name=row["name"],
        channel=row["channel"],
        subject_template=row["subject_template"],
        body_template=row["body_template"],
        created_at=row["created_at"],
    )
    subject, body = tmpl.render(context)
    return {"subject": subject, "body": body, "channel": tmpl.channel}


def save_template(
    name: str,
    channel: str,
    subject_template: str,
    body_template: str,
    db_path: str = DB_PATH,
) -> None:
    """Persist a notification template."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO templates
                (name, channel, subject_template, body_template, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, channel, subject_template, body_template, time.time()),
        )


def list_templates(db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    """Return all stored templates."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        rows = conn.execute("SELECT * FROM templates ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def get_notification(notification_id: str, db_path: str = DB_PATH) -> Optional[Notification]:
    """Fetch a single notification by ID."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM notifications WHERE id = ?", (notification_id,)
        ).fetchone()
    return _row_to_notification(row) if row else None


def get_delivery_log(notification_id: str, db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    """Return delivery attempts for a notification."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM delivery_log WHERE notification_id = ? ORDER BY attempt_at DESC",
            (notification_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def retry_failed(db_path: str = DB_PATH) -> int:
    """Re-attempt delivery for all failed notifications. Returns count retried."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM notifications WHERE status = ?", (NotificationStatus.FAILED.value,)
        ).fetchall()
    notifications = [_row_to_notification(r) for r in rows]
    count = 0
    for notif in notifications:
        notif.retry_count += 1
        if send_notification(notif, db_path):
            count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="BlackRoad Notification Hub – multi-channel notification dispatch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", default=DB_PATH, metavar="PATH")
    sub = parser.add_subparsers(dest="command", required=True)

    # send
    p = sub.add_parser("send", help="Send a notification")
    p.add_argument("recipient")
    p.add_argument("subject")
    p.add_argument("body")
    p.add_argument("--channel", default="email",
                   choices=[c.value for c in Channel], help="Delivery channel")
    p.add_argument("--type", default="general", dest="ntype")

    # batch-send
    p = sub.add_parser("batch-send", help="Send notifications from a JSON file")
    p.add_argument("json_file", help="Path to JSON array of notification objects")

    # read
    p = sub.add_parser("read", help="Mark a notification as read")
    p.add_argument("id", help="Notification ID")

    # unread
    p = sub.add_parser("unread", help="List unread notifications for a recipient")
    p.add_argument("recipient")

    # stats
    p = sub.add_parser("stats", help="Show delivery statistics")
    p.add_argument("--channel", default=None)

    # template-render
    p = sub.add_parser("template-render", help="Render a stored template")
    p.add_argument("name")
    p.add_argument("context_json", help="JSON object of template variables")

    # template-save
    p = sub.add_parser("template-save", help="Save a notification template")
    p.add_argument("name")
    p.add_argument("channel", choices=[c.value for c in Channel])
    p.add_argument("subject_template")
    p.add_argument("body_template")

    # list-templates
    sub.add_parser("list-templates", help="List all templates")

    # retry
    sub.add_parser("retry", help="Retry all failed notifications")

    args = parser.parse_args()
    db = args.db

    if args.command == "send":
        n = Notification.new(
            type=args.ntype,
            recipient=args.recipient,
            subject=args.subject,
            body=args.body,
            channel=Channel(args.channel),
        )
        success = send_notification(n, db)
        print(json.dumps({"id": n.id, "success": success, "status": n.status.value}, indent=2))

    elif args.command == "batch-send":
        with open(args.json_file) as fh:
            items = json.load(fh)
        notifications = [
            Notification.new(
                type=i.get("type", "general"),
                recipient=i["recipient"],
                subject=i["subject"],
                body=i["body"],
                channel=Channel(i.get("channel", "email")),
            )
            for i in items
        ]
        results = batch_send(notifications, db)
        print(json.dumps(results, indent=2))

    elif args.command == "read":
        updated = mark_read(args.id, db)
        print("Marked as read." if updated else "Not found or already read.")

    elif args.command == "unread":
        notifications = get_unread(args.recipient, db)
        print(json.dumps([n.to_dict() for n in notifications], indent=2))

    elif args.command == "stats":
        print(json.dumps(notification_stats(args.channel, db), indent=2))

    elif args.command == "template-render":
        context = json.loads(args.context_json)
        result = template_render(args.name, context, db)
        print(json.dumps(result, indent=2))

    elif args.command == "template-save":
        save_template(args.name, args.channel, args.subject_template, args.body_template, db)
        print(f"Template {args.name!r} saved.")

    elif args.command == "list-templates":
        templates = list_templates(db)
        print(json.dumps(templates, indent=2))

    elif args.command == "retry":
        count = retry_failed(db)
        print(f"Retried {count} failed notification(s).")


if __name__ == "__main__":
    main()
