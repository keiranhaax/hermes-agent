"""Read-receipt tests for PhotonAdapter."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.photon.adapter import PhotonAdapter, _apply_yaml_config


def _make_adapter(
    monkeypatch: pytest.MonkeyPatch, extra: Dict[str, Any] | None = None
) -> PhotonAdapter:
    monkeypatch.setenv("PHOTON_PROJECT_ID", "test-project-id")
    monkeypatch.setenv("PHOTON_PROJECT_SECRET", "test-project-secret")
    return PhotonAdapter(PlatformConfig(enabled=True, token="", extra=extra or {}))


def _capture_sidecar(adapter: PhotonAdapter) -> List[Tuple[str, Dict[str, Any]]]:
    calls: List[Tuple[str, Dict[str, Any]]] = []

    async def _fake_call(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        calls.append((path, body))
        return {"ok": True}

    adapter._sidecar_call = _fake_call  # type: ignore[assignment]
    return calls


def _event() -> Dict[str, Any]:
    return {
        "messageId": "message-1",
        "timestamp": "2026-07-23T00:00:00Z",
        "direction": "inbound",
        "space": {"id": "space-1", "type": "dm", "phone": "+15551234567"},
        "sender": {"id": "+15551234567"},
        "content": {"type": "text", "text": "hello"},
    }


@pytest.mark.asyncio
async def test_mark_read_posts_to_sidecar_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PHOTON_SEND_READ_RECEIPTS", raising=False)
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    assert await adapter._mark_read("space-1", "message-1") is True
    assert calls == [
        ("/read", {"spaceId": "space-1", "messageId": "message-1"})
    ]


@pytest.mark.asyncio
async def test_mark_read_respects_disabled_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PHOTON_SEND_READ_RECEIPTS", raising=False)
    adapter = _make_adapter(monkeypatch, {"send_read_receipts": False})
    calls = _capture_sidecar(adapter)

    assert await adapter._mark_read("space-1", "message-1") is False
    assert calls == []


@pytest.mark.asyncio
async def test_mark_read_env_overrides_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PHOTON_SEND_READ_RECEIPTS", "true")
    adapter = _make_adapter(monkeypatch, {"send_read_receipts": False})
    calls = _capture_sidecar(adapter)

    assert await adapter._mark_read("space-1", "message-1") is True
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_mark_read_failure_is_soft(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _make_adapter(monkeypatch)

    async def _boom(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("sidecar down")

    adapter._sidecar_call = _boom  # type: ignore[assignment]
    assert await adapter._mark_read("space-1", "message-1") is False


@pytest.mark.asyncio
async def test_dispatch_does_not_read_receipt_unauthorized_sender(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    adapter.set_authorization_check(lambda *_: False)
    adapter._mark_read = AsyncMock(return_value=True)  # type: ignore[method-assign]
    adapter.handle_message = AsyncMock()  # type: ignore[method-assign]

    await adapter._dispatch_inbound(_event())
    await asyncio.sleep(0)

    adapter._mark_read.assert_not_awaited()
    adapter.handle_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_schedules_read_receipt_only_after_auth_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    adapter.set_authorization_check(lambda *_: True)
    adapter._mark_read = AsyncMock(return_value=True)  # type: ignore[method-assign]
    adapter.handle_message = AsyncMock()  # type: ignore[method-assign]

    await adapter._dispatch_inbound(_event())
    await asyncio.sleep(0)

    adapter._mark_read.assert_awaited_once_with("space-1", "message-1")
    adapter.handle_message.assert_awaited_once()


def test_yaml_bridge_copies_delivery_signal_settings() -> None:
    assert _apply_yaml_config(
        {}, {"reactions": True, "send_read_receipts": False, "ignored": 1}
    ) == {
        "reactions": True,
        "send_read_receipts": False,
        "gateway_restart_notification": False,
    }