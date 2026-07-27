"""Structured Photon sidecar error contracts."""
from __future__ import annotations

from typing import Any, Dict, cast

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.photon import adapter as photon_adapter
from plugins.platforms.photon.adapter import (
    PhotonAdapter,
    PhotonSidecarError,
    _failed_send,
    _photon_error_kind,
)


def _make_adapter(monkeypatch: pytest.MonkeyPatch) -> PhotonAdapter:
    monkeypatch.setenv("PHOTON_PROJECT_ID", "test-project-id")
    monkeypatch.setenv("PHOTON_PROJECT_SECRET", "test-project-secret")
    adapter = PhotonAdapter(PlatformConfig(enabled=True, token="", extra={}))
    adapter._http_client = cast(Any, object())  # satisfy the connected guard
    return adapter


class _Response:
    status_code = 403

    @staticmethod
    def json() -> Dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": "target_not_allowed",
                "message": "The recipient must initiate the shared-line conversation first.",
                "retryable": False,
                "operationId": "failed:operation-1",
            },
        }


class _Client:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> "_Client":
        return self

    async def __aexit__(self, *args: Any) -> bool:
        return False

    async def post(self, *args: Any, **kwargs: Any) -> _Response:
        return _Response()


@pytest.mark.asyncio
async def test_sidecar_call_parses_structured_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _make_adapter(monkeypatch)
    monkeypatch.setattr(photon_adapter.httpx, "AsyncClient", _Client)

    with pytest.raises(PhotonSidecarError) as caught:
        await adapter._sidecar_call("/send", {"spaceId": "s", "text": "hi"})

    assert caught.value.code == "target_not_allowed"
    assert caught.value.retryable is False
    assert caught.value.operation_id == "failed:operation-1"
    assert "initiate" in str(caught.value)


def test_structured_error_becomes_machine_readable_send_result() -> None:
    result = _failed_send(
        PhotonSidecarError(
            "quota_exceeded", "Photon quota reached.", retryable=True
        )
    )

    assert result.success is False
    assert result.retryable is True
    assert result.error_kind == "rate_limited"
    assert result.raw_response == {"photon_error_code": "quota_exceeded"}


@pytest.mark.parametrize(
    ("code", "kind"),
    [
        ("target_not_allowed", "forbidden"),
        ("managed_line_target", "bad_format"),
        ("quota_exceeded", "rate_limited"),
        ("not_found", "not_found"),
        ("unsupported", "bad_format"),
        ("auth_failed", "forbidden"),
        ("upstream_unavailable", "transient"),
        ("internal_error", "unknown"),
    ],
)
def test_all_normalized_error_codes_map_stably(code: str, kind: str) -> None:
    assert _photon_error_kind(code) == kind


@pytest.mark.asyncio
async def test_sidecar_call_handles_legacy_string_error_without_exposing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class LegacyResponse(_Response):
        status_code = 400

        @staticmethod
        def json() -> Dict[str, Any]:
            return {
                "ok": False,
                "error": "legacy provider stack: /home/alice/private-file.jpg",
            }

    class LegacyClient(_Client):
        async def post(self, *args: Any, **kwargs: Any) -> _Response:
            return LegacyResponse()

    adapter = _make_adapter(monkeypatch)
    monkeypatch.setattr(photon_adapter.httpx, "AsyncClient", LegacyClient)

    with pytest.raises(PhotonSidecarError) as caught:
        await adapter._sidecar_call("/send", {})

    assert caught.value.code == "internal_error"
    assert str(caught.value) == "Photon sidecar reported a failure."
    assert "/home/alice" not in str(caught.value)
