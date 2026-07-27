"""Photon CLI status diagnostics."""
from __future__ import annotations

import argparse

from plugins.platforms.photon import cli as photon_cli


def test_status_reports_listener_plan_mode_and_quotas(monkeypatch, capsys) -> None:
    monkeypatch.setattr(photon_cli, "_refresh_status_numbers", lambda: None)
    monkeypatch.setattr(photon_cli.photon_auth, "print_credential_summary", lambda emit: emit("credentials ok"))
    monkeypatch.setattr(photon_cli.shutil, "which", lambda name: "/usr/bin/node")
    monkeypatch.setattr(photon_cli, "_sidecar_is_listening", lambda port: True)
    monkeypatch.setattr(
        photon_cli.photon_auth,
        "load_project_credentials",
        lambda: ("project", "secret"),
    )
    monkeypatch.setattr(
        photon_cli.photon_auth,
        "get_subscription_details",
        lambda *args: {"tier": "free", "status": "active"},
    )
    monkeypatch.setattr(
        photon_cli.photon_auth,
        "get_imessage_service_info",
        lambda *args: {"type": "shared"},
    )

    assert photon_cli._cmd_status(argparse.Namespace()) == 0
    out = capsys.readouterr().out
    assert "sidecar listener" in out
    assert "free (active)" in out
    assert "iMessage line mode  : shared" in out
    assert "5,000 messages/day" in out
    assert "50 new conversations/line/day" in out
