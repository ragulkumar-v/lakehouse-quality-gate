"""
Webhook alerting for quality-gate failures.

Kept as a small, swappable abstraction: `WebhookNotifier.send()` really POSTs
JSON to GE_WEBHOOK_URL (e.g. a Slack incoming webhook, PagerDuty events API,
or an internal on-call endpoint) when that env var is set to an http(s) URL.
When it isn't set -- the default in local dev, CI, and the test suite -- the
notifier writes the exact same alert payload to a local JSON file instead, so
nothing here ever needs live network access to be exercised end-to-end.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

DEFAULT_ALERT_SINK = Path(__file__).resolve().parents[1] / "data" / "alerts" / "last_alert.json"


@dataclass
class WebhookDeliveryResult:
    delivered_via: str  # "http" or "local_file"
    destination: str
    ok: bool


class WebhookNotifier:
    def __init__(self, url: str | None = None, sink_path: Path | None = None, timeout_seconds: float = 5.0):
        self.url = url if url is not None else os.environ.get("GE_WEBHOOK_URL")
        self.sink_path = sink_path or DEFAULT_ALERT_SINK
        self.timeout_seconds = timeout_seconds

    def send(self, payload: dict[str, Any]) -> WebhookDeliveryResult:
        if self.url:
            response = requests.post(self.url, json=payload, timeout=self.timeout_seconds)
            response.raise_for_status()
            return WebhookDeliveryResult(delivered_via="http", destination=self.url, ok=True)

        self.sink_path.parent.mkdir(parents=True, exist_ok=True)
        self.sink_path.write_text(json.dumps(payload, indent=2, default=str))
        return WebhookDeliveryResult(delivered_via="local_file", destination=str(self.sink_path), ok=True)
