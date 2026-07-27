"""Free/shared-line rich iMessage action tests."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from plugins.platforms.photon import tools as photon_tools
from plugins.platforms.photon.adapter import PhotonAdapter, PhotonSidecarError


def _make_adapter(monkeypatch: pytest.MonkeyPatch) -> PhotonAdapter:
    monkeypatch.setenv("PHOTON_PROJECT_ID", "test-project-id")
    monkeypatch.setenv("PHOTON_PROJECT_SECRET", "test-project-secret")
    return PhotonAdapter(
        PlatformConfig(
            enabled=True,
            token="",
            extra={
                "mini_app": {
                    "app_name": "Hermes",
                    "extension_bundle_id": "codes.photon.hermes",
                    "team_id": "ABCDE12345",
                    "allowed_url_hosts": ["example.com"],
                }
            },
        )
    )


def _capture_sidecar(adapter: PhotonAdapter) -> List[Tuple[str, Dict[str, Any]]]:
    calls: List[Tuple[str, Dict[str, Any]]] = []

    async def _fake_call(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        calls.append((path, body))
        return {"ok": True, "messageId": "rich-1", "state": "accepted"}

    adapter._sidecar_call = _fake_call  # type: ignore[assignment]
    return calls


@pytest.mark.asyncio
async def test_adapter_rich_methods_use_sidecar_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    effect = await adapter.send_effect("space-1", "hello", "confetti")
    contact = await adapter.share_contact_card("space-1")
    custom = await adapter.send_custom_app_card(
        "space-1",
        {
            "url": "https://example.com/status",
            "layout": {"caption": "Ready"},
        },
    )

    assert effect.success and effect.message_id == "rich-1"
    assert contact["state"] == "accepted"
    assert custom.success
    assert [path for path, _ in calls] == [
        "/effect",
        "/contact-card",
        "/custom-app-card",
    ]


@pytest.mark.asyncio
async def test_custom_app_card_accepts_safe_jpeg_preview(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)
    preview = tmp_path / "preview.jpg"
    preview.write_bytes(b"\xff\xd8\xff\xe0jpeg-preview")
    monkeypatch.setattr(
        adapter,
        "validate_media_delivery_path",
        lambda path: path if path == str(preview) else None,
    )

    result = await adapter.send_custom_app_card(
        "space-1",
        {
            "url": "https://example.com/status",
            "layout": {"caption": "Ready", "imageTitle": "Status"},
            "imagePath": str(preview),
        },
    )

    assert result.success is True
    body = calls[0][1]
    assert "imagePath" not in body
    assert body["layout"]["imageBase64"]
    assert body["layout"]["imageTitle"] == "Status"


@pytest.mark.asyncio
async def test_custom_app_card_read_error_does_not_expose_local_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    private_path = "/home/alice/private-preview.jpg"
    monkeypatch.setattr(
        adapter,
        "validate_media_delivery_path",
        lambda path: path if path == private_path else None,
    )

    def _raise_read_error(_path: Any) -> bytes:
        raise OSError(f"permission denied: {private_path}")

    monkeypatch.setattr(photon_tools.Path, "read_bytes", _raise_read_error)
    result = await adapter.send_custom_app_card(
        "space-1",
        {
            "url": "https://example.com/status",
            "layout": {"caption": "Ready", "imageTitle": "Status"},
            "imagePath": private_path,
        },
    )

    assert result.success is False
    assert result.error == "could not read app-card image"
    assert private_path not in (result.error or "")


@pytest.mark.asyncio
async def test_custom_app_card_requires_operator_identity_and_host_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PHOTON_PROJECT_ID", "test-project-id")
    monkeypatch.setenv("PHOTON_PROJECT_SECRET", "test-project-secret")
    unconfigured = PhotonAdapter(PlatformConfig(enabled=True, token="", extra={}))
    calls = _capture_sidecar(unconfigured)

    missing = await unconfigured.send_custom_app_card(
        "space-1",
        {"url": "https://example.com/card", "layout": {"caption": "Ready"}},
    )
    assert missing.success is False
    assert calls == []

    configured = _make_adapter(monkeypatch)
    configured_calls = _capture_sidecar(configured)
    blocked = await configured.send_custom_app_card(
        "space-1",
        {"url": "https://evil.example/card", "layout": {"caption": "Ready"}},
    )
    assert blocked.success is False
    assert configured_calls == []


@pytest.mark.asyncio
async def test_contact_card_is_rate_limited_per_chat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_sidecar(adapter)

    await adapter.share_contact_card("space-1")
    with pytest.raises(PhotonSidecarError, match="already shared"):
        await adapter.share_contact_card("space-1")

    assert [path for path, _ in calls] == ["/contact-card"]


class _FakeLiveAdapter:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []

    async def send_effect(self, *args: Any, **kwargs: Any) -> SendResult:
        self.calls.append(("effect", args, kwargs))
        return SendResult(success=True, message_id="m-effect")

    async def share_contact_card(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("contact", args, kwargs))
        return {"ok": True, "state": "accepted"}

    async def send_custom_app_card(self, *args: Any, **kwargs: Any) -> SendResult:
        self.calls.append(("custom", args, kwargs))
        return SendResult(success=True, message_id="m-custom")


def test_plugin_tool_dispatches_effect(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeLiveAdapter()
    monkeypatch.setattr(photon_tools, "_live_adapter", lambda: fake)

    result = json.loads(
        photon_tools.handle_photon_imessage(
            {
                "action": "effect",
                "target": "+15551234567",
                "text": "hello",
                "effect": "confetti",
            }
        )
    )

    assert result["success"] is True
    assert result["message_id"] == "m-effect"
    assert fake.calls[0][0] == "effect"


def test_plugin_tool_rejects_invalid_custom_card_before_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeLiveAdapter()
    monkeypatch.setattr(photon_tools, "_live_adapter", lambda: fake)

    result = json.loads(
        photon_tools.handle_photon_imessage(
            {
                "action": "custom_app_card",
                "target": "+15551234567",
                "url": "file:///etc/passwd",
                "layout": {"caption": "Status"},
            }
        )
    )

    assert result["success"] is False
    assert fake.calls == []


def test_plugin_tool_hides_unexpected_exception_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeLiveAdapter()

    async def _fail(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError("permission denied: /home/alice/private-card.jpg")

    fake.share_contact_card = _fail  # type: ignore[method-assign]
    monkeypatch.setattr(photon_tools, "_live_adapter", lambda: fake)

    result = json.loads(
        photon_tools.handle_photon_imessage(
            {"action": "share_contact", "target": "+155****4567"}
        )
    )

    assert result == {"success": False, "error": "Photon operation failed"}


def test_tool_schema_has_no_business_only_actions() -> None:
    actions = set(
        photon_tools.PHOTON_IMESSAGE_SCHEMA["parameters"]["properties"][
            "action"
        ]["enum"]
    )
    business_only = {
        "create_group",
        "manage_group",
        "rename_group",
        "add_member",
        "remove_member",
        "set_group_avatar",
        "set_group_background",
        "route_phone",
        "route_line",
        "manage_dedicated_lines",
        "group_change_events",
        "auto_scale_lines",
    }
    assert actions.isdisjoint(business_only)
    # Spectrum's generic app() builder performs its own metadata fetch. Keep it
    # absent; customized cards use supplied layout data and do not need that fetch.
    assert "app_card" not in actions


def test_photon_toolset_is_scoped_to_photon_platform() -> None:
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins
    from hermes_cli.tools_config import _get_platform_tools

    discover_plugins()
    platform_registry.get("photon")
    assert "photon" in _get_platform_tools({}, "photon")
    assert "photon" not in _get_platform_tools({}, "telegram")
    assert "photon" not in _get_platform_tools({}, "cli")


def test_app_card_url_validation_blocks_local_and_private_targets() -> None:
    assert photon_tools._http_url("http://127.0.0.1/admin") is None
    assert photon_tools._http_url("http://169.254.169.254/latest/meta-data") is None
    assert photon_tools._http_url("http://service.internal/status") is None
    assert photon_tools._http_url("http://example.com/card") is None
    assert photon_tools._http_url("https://user:pass@example.com/card") is None
    assert photon_tools._http_url("https://[::1]/card") is None
    assert photon_tools._http_url("https://example.com/card") == "https://example.com/card"
