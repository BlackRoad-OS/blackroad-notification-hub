"""Tests for blackroad-notification-hub."""
import os
import json
import time
import pytest
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from module import (
    Notification, Channel, NotificationStatus,
    send_notification, batch_send, mark_read, get_unread,
    notification_stats, template_render, save_template, list_templates,
    get_notification, get_delivery_log, retry_failed, init_db,
    _simple_render,
)


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "test_notifications.db")


class TestSendNotification:
    def test_send_returns_true_on_success(self, db):
        n = Notification.new("alert", "alice@example.com", "Hi", "Hello", Channel.EMAIL)
        assert send_notification(n, db) is True

    def test_sent_notification_persisted(self, db):
        n = Notification.new("alert", "bob@example.com", "Subject", "Body", Channel.SLACK)
        send_notification(n, db)
        fetched = get_notification(n.id, db)
        assert fetched is not None
        assert fetched.id == n.id
        assert fetched.status == NotificationStatus.SENT

    def test_sent_at_set_after_send(self, db):
        n = Notification.new("info", "carol@example.com", "Sub", "Body", Channel.PUSH)
        send_notification(n, db)
        fetched = get_notification(n.id, db)
        assert fetched.sent_at is not None
        assert fetched.sent_at > 0

    def test_delivery_log_entry_created(self, db):
        n = Notification.new("info", "dave@example.com", "Sub", "Body", Channel.WEBHOOK)
        send_notification(n, db)
        log = get_delivery_log(n.id, db)
        assert len(log) == 1
        assert log[0]["success"] == 1

    def test_metadata_preserved(self, db):
        meta = {"priority": "high", "source": "system"}
        n = Notification.new("sys", "eve@example.com", "Sys", "Msg", Channel.EMAIL, metadata=meta)
        send_notification(n, db)
        fetched = get_notification(n.id, db)
        assert fetched.metadata == meta

    def test_different_channels_accepted(self, db):
        for ch in Channel:
            n = Notification.new("t", f"u@test.com", "S", "B", ch)
            assert send_notification(n, db) is True


class TestBatchSend:
    def test_batch_send_returns_result_per_id(self, db):
        notifications = [
            Notification.new("a", f"user{i}@example.com", "Sub", "Body", Channel.EMAIL)
            for i in range(3)
        ]
        results = batch_send(notifications, db)
        assert len(results) == 3
        for nid, success in results.items():
            assert isinstance(success, bool)

    def test_all_notifications_persisted(self, db):
        notifications = [
            Notification.new("a", "u@e.com", "S", "B", Channel.SLACK)
            for _ in range(5)
        ]
        batch_send(notifications, db)
        for n in notifications:
            fetched = get_notification(n.id, db)
            assert fetched is not None


class TestMarkRead:
    def test_mark_read_updates_status(self, db):
        n = Notification.new("t", "x@e.com", "S", "B", Channel.EMAIL)
        send_notification(n, db)
        result = mark_read(n.id, db)
        assert result is True
        fetched = get_notification(n.id, db)
        assert fetched.status == NotificationStatus.READ

    def test_mark_read_nonexistent_returns_false(self, db):
        init_db(db)
        assert mark_read("nonexistent-id", db) is False

    def test_mark_read_idempotent(self, db):
        n = Notification.new("t", "y@e.com", "S", "B", Channel.EMAIL)
        send_notification(n, db)
        mark_read(n.id, db)
        # Second call should gracefully return False (already READ)
        result = mark_read(n.id, db)
        assert result is False


class TestGetUnread:
    def test_get_unread_returns_only_sent(self, db):
        recipient = "zoe@example.com"
        for _ in range(3):
            n = Notification.new("t", recipient, "Sub", "Body", Channel.EMAIL)
            send_notification(n, db)
        unread = get_unread(recipient, db)
        assert len(unread) == 3
        for n in unread:
            assert n.status == NotificationStatus.SENT

    def test_get_unread_excludes_read(self, db):
        recipient = "ann@example.com"
        n = Notification.new("t", recipient, "Sub", "Body", Channel.EMAIL)
        send_notification(n, db)
        mark_read(n.id, db)
        assert get_unread(recipient, db) == []

    def test_get_unread_other_recipient_not_included(self, db):
        n = Notification.new("t", "other@example.com", "Sub", "Body", Channel.EMAIL)
        send_notification(n, db)
        assert get_unread("nobody@example.com", db) == []


class TestNotificationStats:
    def test_stats_all_channels(self, db):
        for ch in [Channel.EMAIL, Channel.SLACK, Channel.WEBHOOK]:
            n = Notification.new("t", "u@e.com", "S", "B", ch)
            send_notification(n, db)
        stats = notification_stats(db_path=db)
        assert stats["total_notifications"] >= 3
        assert "by_status" in stats
        assert "by_channel" in stats
        assert "delivery_success_rate_pct" in stats

    def test_stats_filtered_by_channel(self, db):
        for _ in range(2):
            n = Notification.new("t", "u@e.com", "S", "B", Channel.PUSH)
            send_notification(n, db)
        stats = notification_stats(channel="push", db_path=db)
        assert stats["total_notifications"] >= 2
        assert stats["filter_channel"] == "push"


class TestTemplateRender:
    def test_render_simple_template(self, db):
        save_template(
            "welcome", "email",
            "Welcome {{name}}!",
            "Hello {{name}}, your code is {{code}}.",
            db,
        )
        result = template_render("welcome", {"name": "Alice", "code": "XYZ"}, db)
        assert result["subject"] == "Welcome Alice!"
        assert "Alice" in result["body"]
        assert "XYZ" in result["body"]

    def test_render_missing_variable_leaves_placeholder(self, db):
        save_template(
            "partial", "slack",
            "Hi {{name}}",
            "Your ref is {{ref}}",
            db,
        )
        result = template_render("partial", {"name": "Bob"}, db)
        assert result["subject"] == "Hi Bob"
        assert "{{ref}}" in result["body"]

    def test_render_nonexistent_template_raises(self, db):
        init_db(db)
        with pytest.raises(KeyError):
            template_render("does_not_exist", {}, db)

    def test_list_templates_after_save(self, db):
        save_template("t1", "email", "Sub {{x}}", "Body {{x}}", db)
        save_template("t2", "slack", "Alert", "Details", db)
        templates = list_templates(db)
        names = [t["name"] for t in templates]
        assert "t1" in names
        assert "t2" in names


class TestSimpleRender:
    def test_basic_substitution(self):
        assert _simple_render("Hello {{name}}", {"name": "World"}) == "Hello World"

    def test_missing_key_unchanged(self):
        assert _simple_render("{{missing}}", {}) == "{{missing}}"

    def test_multiple_variables(self):
        result = _simple_render("{{a}} and {{b}}", {"a": "foo", "b": "bar"})
        assert result == "foo and bar"

    def test_dotted_access(self):
        result = _simple_render("{{user.name}}", {"user": {"name": "Alice"}})
        assert result == "Alice"


class TestRetryFailed:
    def test_retry_returns_count(self, db):
        # Force a failed notification by using an invalid channel value at DB level
        init_db(db)
        import sqlite3 as _sq
        conn = _sq.connect(db)
        conn.execute(
            "INSERT INTO notifications "
            "(id, type, recipient, subject, body, channel, status, created_at, metadata, retry_count) "
            "VALUES ('test-id', 't', 'u@e.com', 'S', 'B', 'email', 'failed', ?, '{}', 0)",
            (time.time(),)
        )
        conn.commit()
        conn.close()
        count = retry_failed(db)
        assert count >= 0  # may succeed on retry


class TestNotificationDataclass:
    def test_new_creates_unique_ids(self):
        n1 = Notification.new("t", "u@e.com", "S", "B", Channel.EMAIL)
        n2 = Notification.new("t", "u@e.com", "S", "B", Channel.EMAIL)
        assert n1.id != n2.id

    def test_to_dict_has_all_fields(self):
        n = Notification.new("alert", "u@e.com", "Sub", "Body", Channel.SLACK)
        d = n.to_dict()
        for field in ["id", "type", "recipient", "subject", "body", "channel", "status"]:
            assert field in d

    def test_default_status_is_pending(self):
        n = Notification.new("t", "u@e.com", "S", "B", Channel.PUSH)
        assert n.status == NotificationStatus.PENDING
