"""Unit tests for quality/webhook.py -- no network involved either way."""

from __future__ import annotations

import json
from pathlib import Path

from quality.webhook import WebhookNotifier


def test_send_without_url_writes_local_json_file(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("GE_WEBHOOK_URL", raising=False)
    sink = tmp_path / "alerts" / "last_alert.json"
    notifier = WebhookNotifier(sink_path=sink)

    result = notifier.send({"hello": "world", "count": 3})

    assert result.delivered_via == "local_file"
    assert result.ok is True
    assert sink.exists()
    assert json.loads(sink.read_text()) == {"hello": "world", "count": 3}


def test_send_with_url_posts_json_and_never_touches_the_network(tmp_path: Path, monkeypatch):
    """
    Proves the http path is wired up correctly WITHOUT making a real network
    call: `requests.post` is monkeypatched to a stub that records its
    arguments and returns a fake 200 response.
    """
    calls = {}

    class _FakeResponse:
        def raise_for_status(self):
            return None

    def _fake_post(url, json=None, timeout=None):
        calls["url"] = url
        calls["json"] = json
        calls["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr("quality.webhook.requests.post", _fake_post)

    notifier = WebhookNotifier(url="https://hooks.example.com/alert", sink_path=tmp_path / "unused.json")
    payload = {"gate": "great_expectations_checkpoint", "success": False}
    result = notifier.send(payload)

    assert result.delivered_via == "http"
    assert result.destination == "https://hooks.example.com/alert"
    assert result.ok is True
    assert calls["url"] == "https://hooks.example.com/alert"
    assert calls["json"] == payload
    assert not (tmp_path / "unused.json").exists(), "http path must not also write the local sink"


def test_env_var_selects_url_when_not_passed_explicitly(monkeypatch, tmp_path):
    monkeypatch.setenv("GE_WEBHOOK_URL", "https://from-env.example.com/hook")
    notifier = WebhookNotifier(sink_path=tmp_path / "unused.json")
    assert notifier.url == "https://from-env.example.com/hook"


def test_explicit_url_wins_over_env_var(monkeypatch, tmp_path):
    monkeypatch.setenv("GE_WEBHOOK_URL", "https://from-env.example.com/hook")
    notifier = WebhookNotifier(url="https://explicit.example.com/hook", sink_path=tmp_path / "unused.json")
    assert notifier.url == "https://explicit.example.com/hook"
