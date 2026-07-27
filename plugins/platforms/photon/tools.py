"""Agent-facing rich iMessage operations for the Photon platform plugin."""
from __future__ import annotations

import ipaddress
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from agent.redact import redact_sensitive_text
from gateway.config import Platform, load_gateway_config
from gateway.platforms.base import BasePlatformAdapter

_EFFECTS = (
    "slam",
    "loud",
    "gentle",
    "invisible_ink",
    "confetti",
    "fireworks",
    "balloons",
    "heart",
    "lasers",
    "celebration",
    "sparkles",
    "spotlight",
    "echo",
)
_E164_RE = re.compile(r"^\+[1-9]\d{6,14}$")
_SAFE_ERROR_CODES = frozenset(
    {
        "target_not_allowed",
        "managed_line_target",
        "quota_exceeded",
        "not_found",
        "unsupported",
        "auth_failed",
        "upstream_unavailable",
        "invalid_request",
        "attachment_too_large",
        "internal_error",
    }
)


PHOTON_IMESSAGE_SCHEMA = {
    "name": "photon_imessage",
    "description": (
        "Use Photon-specific iMessage capabilities on the live gateway: native "
        "effects, contact-card sharing, configured mini-app cards, edits/unsend, and honest "
        "message/recipient status. This tool does not expose Business-only "
        "dedicated-line or group-management operations."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": [
                    "effect",
                    "share_contact",
                    "custom_app_card",
                    "edit",
                    "unsend",
                    "message_status",
                    "recipient_status",
                ],
            },
            "target": {
                "type": "string",
                "description": "Photon space id or E.164 recipient. Defaults to the Photon home channel.",
            },
            "message_id": {
                "type": "string",
                "description": "Outbound message id for edit, unsend, or message_status.",
            },
            "text": {"type": "string", "maxLength": 8000},
            "effect": {"type": "string", "enum": list(_EFFECTS)},
            "reply_to": {"type": "string"},
            "url": {"type": "string", "maxLength": 4096},
            "image_path": {
                "type": "string",
                "description": "Optional safe local JPEG preview path for custom_app_card.",
            },
            "image_title": {"type": "string", "maxLength": 500},
            "image_subtitle": {"type": "string", "maxLength": 500},
            "layout": {
                "type": "object",
                "properties": {
                    "caption": {"type": "string", "maxLength": 1000},
                    "subcaption": {"type": "string", "maxLength": 1000},
                    "trailingCaption": {"type": "string", "maxLength": 1000},
                    "trailingSubcaption": {"type": "string", "maxLength": 1000},
                    "summary": {"type": "string", "maxLength": 1000},
                },
                "additionalProperties": False,
            },
        },
        "required": ["action"],
        "additionalProperties": False,
    },
}


def _error(message: str) -> str:
    return json.dumps(
        {"success": False, "error": redact_sensitive_text(str(message))}
    )


def _http_url(value: Any) -> Optional[str]:
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        parsed = urlparse(value)
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    hostname = (parsed.hostname or "").lower()
    if (
        hostname == "localhost"
        or hostname.endswith(".localhost")
        or hostname.endswith(".local")
        or hostname.endswith(".internal")
    ):
        return None
    try:
        if not ipaddress.ip_address(hostname).is_global:
            return None
    except ValueError:
        pass
    return value


def _live_adapter() -> Any:
    try:
        from gateway.run import _gateway_runner_ref

        runner = _gateway_runner_ref()
    except Exception:
        runner = None
    adapter = runner.adapters.get(Platform("photon")) if runner is not None else None
    if adapter is None:
        raise RuntimeError("Photon requires a live gateway adapter")
    return adapter


def _target(args: Dict[str, Any]) -> Optional[str]:
    explicit = str(args.get("target") or "").strip()
    if explicit:
        if len(explicit) > 512 or any(ord(char) < 32 for char in explicit):
            return None
        return explicit
    config = load_gateway_config()
    home = config.get_home_channel(Platform("photon"))
    return str(home.chat_id) if home and home.chat_id else None


def _result_payload(result: Any) -> Dict[str, Any]:
    if isinstance(result, dict):
        return result
    raw = getattr(result, "raw_response", None)
    error = getattr(result, "error", None)
    details = (
        {
            key: raw[key]
            for key in ("photon_error_code", "photon_operation_id")
            if key in raw
        }
        if isinstance(raw, dict)
        else {}
    )
    return {
        "success": bool(getattr(result, "success", False)),
        "message_id": getattr(result, "message_id", None),
        "error": redact_sensitive_text(str(error)) if error is not None else None,
        "error_kind": getattr(result, "error_kind", None),
        "details": details or None,
    }


def _custom_card(args: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = _http_url(args.get("url"))
    layout = args.get("layout")
    if not url:
        return None, "custom_app_card requires a valid HTTPS url"
    if not isinstance(layout, dict) or not any(
        str(layout.get(key) or "").strip()
        for key in ("caption", "subcaption", "trailingCaption", "trailingSubcaption")
    ):
        return None, "layout requires at least one visible caption field"
    if any(
        len(str(layout.get(key) or "")) > 1000
        for key in (
            "caption",
            "subcaption",
            "trailingCaption",
            "trailingSubcaption",
            "summary",
        )
    ):
        return None, "layout text fields must be 1000 characters or fewer"
    card: Dict[str, Any] = {
        "url": url,
        "layout": dict(layout),
    }
    image_path = str(args.get("image_path") or "").strip()
    image_title = str(args.get("image_title") or "").strip()
    image_subtitle = str(args.get("image_subtitle") or "").strip()
    if len(image_title) > 500 or len(image_subtitle) > 500:
        return None, "image title fields must be 500 characters or fewer"
    if (image_title or image_subtitle) and not image_path:
        return None, "image_title/image_subtitle require image_path"
    if image_path:
        if not image_title:
            return None, "image_title is required with image_path"
        safe_path = BasePlatformAdapter.validate_media_delivery_path(image_path)
        if not safe_path or Path(safe_path).suffix.lower() not in {".jpg", ".jpeg"}:
            return None, "image_path must be a safe local JPEG file"
        card["imagePath"] = safe_path
        card["layout"]["imageTitle"] = image_title
        if image_subtitle:
            card["layout"]["imageSubtitle"] = image_subtitle

    if args.get("reply_to"):
        card["replyTo"] = str(args["reply_to"])
    return card, None


def handle_photon_imessage(args: Dict[str, Any], **_: Any) -> str:
    """Dispatch a validated Photon-specific action through the live adapter."""
    action = str(args.get("action") or "")
    message_id = str(args.get("message_id") or "").strip()
    if len(message_id) > 512 or any(ord(char) < 32 for char in message_id):
        return _error("message_id is invalid")
    try:
        adapter = _live_adapter()
        from model_tools import _run_async

        if action == "message_status":
            if not message_id:
                return _error("message_status requires message_id")
            return json.dumps(_run_async(adapter.get_message_status(message_id)))

        target = _target(args)
        if not target:
            return _error("No Photon target or home channel is configured")

        if action == "recipient_status":
            if not _E164_RE.fullmatch(target):
                return _error("recipient_status target must be E.164")
            return json.dumps(_run_async(adapter.get_recipient_status(target)))

        if action == "effect":
            text = str(args.get("text") or "")
            effect = str(args.get("effect") or "")
            if not text or len(text) > 8000 or effect not in _EFFECTS:
                return _error("effect requires text and a supported effect name")
            result = _run_async(
                adapter.send_effect(
                    target,
                    text,
                    effect,
                    reply_to=str(args.get("reply_to") or "") or None,
                )
            )
            return json.dumps(_result_payload(result))

        if action == "share_contact":
            return json.dumps(_run_async(adapter.share_contact_card(target)))


        if action == "custom_app_card":
            card, error = _custom_card(args)
            if error or card is None:
                return _error(error or "invalid custom app card")
            result = _run_async(adapter.send_custom_app_card(target, card))
            return json.dumps(_result_payload(result))

        if action == "edit":
            text = str(args.get("text") or "")
            if not message_id or not text or len(text) > 8000:
                return _error("edit requires message_id and text")
            return json.dumps(
                _result_payload(
                    _run_async(adapter.edit_message(target, message_id, text, finalize=True))
                )
            )

        if action == "unsend":
            if not message_id:
                return _error("unsend requires message_id")
            success = _run_async(adapter.delete_message(target, message_id))
            return json.dumps({"success": bool(success), "message_id": message_id})

        return _error(f"Unsupported Photon action: {action}")
    except Exception as exc:
        code = str(getattr(exc, "code", ""))
        if code in _SAFE_ERROR_CODES:
            return json.dumps(
                {
                    "success": False,
                    "error": redact_sensitive_text(str(exc)),
                    "error_code": code,
                    "retryable": bool(getattr(exc, "retryable", False)),
                }
            )
        return _error("Photon operation failed")
