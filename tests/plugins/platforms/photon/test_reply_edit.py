"""Native reply, edit, unsend, and streaming contracts for Photon."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.photon.adapter import PhotonAdapter


def _make_adapter(monkeypatch: pytest.MonkeyPatch) -> PhotonAdapter:
    monkeypatch.setenv("PHOTON_PROJECT_ID", "test-project-id")
    monkeypatch.setenv("PHOTON_PROJECT_SECRET", "test-project-secret")
    return PhotonAdapter(PlatformConfig(enabled=True, token="", extra={}))


def _capture_sidecar(adapter: PhotonAdapter) -> List[Tuple[str, Dict[str, Any]]]:
    calls: List[Tuple[str, Dict[str, Any]]] = []

    async def _fake_call(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        calls.append((path, body))
        return {"ok": True, "messageId": body.get("messageId") or "out-1"}

    adapter._sidecar_call = _fake_call  # type: ignore[assignment]
    return calls


@pytest.mark.asyncio
async def test_send_forwards_native_reply_target(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    result = await adapter.send("space-1", "reply", reply_to="in-1")

    assert result.success is True
    assert calls == [
        (
            "/send",
            {
                "spaceId": "space-1",
                "text": "reply",
                "format": "markdown",
                "replyTo": "in-1",
            },
        )
    ]


@pytest.mark.asyncio
async def test_edit_message_uses_sidecar_and_keeps_message_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    result = await adapter.edit_message(
        "space-1", "out-1", "updated", finalize=True
    )

    assert result.success is True
    assert result.message_id == "out-1"
    assert calls == [
        (
            "/edit",
            {"spaceId": "space-1", "messageId": "out-1", "text": "updated"},
        )
    ]


@pytest.mark.asyncio
async def test_delete_message_unsends(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    assert await adapter.delete_message("space-1", "out-1") is True
    assert calls == [
        ("/unsend", {"spaceId": "space-1", "messageId": "out-1"})
    ]


@pytest.mark.asyncio
async def test_delete_message_failure_is_soft(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _make_adapter(monkeypatch)

    async def _boom(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("outside unsend window")

    adapter._sidecar_call = _boom  # type: ignore[assignment]
    assert await adapter.delete_message("space-1", "out-1") is False


@pytest.mark.asyncio
async def test_streaming_edit_uses_plain_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    await adapter.edit_message(
        "space-1", "out-stream", "**bold** and `code`", finalize=True
    )

    assert calls == [
        (
            "/edit",
            {
                "spaceId": "space-1",
                "messageId": "out-stream",
                "text": "bold and code",
            },
        )
    ]
