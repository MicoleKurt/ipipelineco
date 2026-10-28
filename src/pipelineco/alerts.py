"""Alerts go to three places: the console, ops.alerts (so the report can show them), and an
optional Slack-compatible webhook set with PIPELINECO_WEBHOOK_URL."""

from __future__ import annotations

import json
import os
import urllib.request

from .warehouse import Warehouse, utcnow


class AlertSink:
    def __init__(self, wh: Warehouse, run_id: str, console=None, webhook: str | None = None):
        self.wh = wh
        self.run_id = run_id
        self.console = console
        self.webhook = webhook if webhook is not None else os.environ.get("PIPELINECO_WEBHOOK_URL")
        self.sent: list[tuple[str, str]] = []

    def send(self, level: str, message: str) -> None:
        self.sent.append((level, message))
        self.wh.insert_row("ops.alerts", {"run_id": self.run_id, "ts": utcnow(), "level": level, "message": message})
        if self.console is not None:
            style = {"error": "bold red", "warning": "yellow"}.get(level, "cyan")
            self.console.print(f"    [{style}]ALERT {level.upper()}[/] {message}")
        if self.webhook:
            body = json.dumps({"text": f"[pipelineco {level}] {message} (run {self.run_id})"}).encode()
            req = urllib.request.Request(self.webhook, data=body, headers={"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(req, timeout=5).read()
            except Exception as exc:  # an alert channel outage must never fail the pipeline
                if self.console is not None:
                    self.console.print(f"    [dim]webhook delivery failed: {exc}[/]")
