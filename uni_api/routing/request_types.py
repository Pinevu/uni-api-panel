from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional


COMPACTION_REQUEST_TYPE = "compaction"
VISION_REQUEST_TYPE = "vision"
COMPACTION_TRIGGER_INPUT_TYPE = "compaction_trigger"
_IMAGE_CONTENT_TYPES = frozenset({"image", "image_url", "input_image"})


def normalize_request_type(value: Any) -> str:
    return str(value or "").strip().lower()


def request_type_values(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    values = (value,) if isinstance(value, str) else value
    if not isinstance(values, (list, tuple, set, frozenset)):
        values = (values,)
    normalized = []
    for item in values:
        request_type = normalize_request_type(item)
        if request_type and request_type not in normalized:
            normalized.append(request_type)
    return tuple(normalized)


def provider_request_type_values(provider: Mapping[str, Any], key: str) -> tuple[str, ...]:
    values = []
    values.extend(request_type_values(provider.get(key)))
    preferences = provider.get("preferences")
    if isinstance(preferences, Mapping):
        values.extend(request_type_values(preferences.get(key)))
    return tuple(dict.fromkeys(values))


def provider_accepts_request_type(
    provider: Mapping[str, Any],
    request_type: Optional[str],
) -> bool:
    normalized_request_type = normalize_request_type(request_type)
    only_request_types = provider_request_type_values(provider, "only_request_types")
    if only_request_types and normalized_request_type not in only_request_types:
        return False

    exclude_request_types = provider_request_type_values(provider, "exclude_request_types")
    return not (
        normalized_request_type
        and normalized_request_type in exclude_request_types
    )


def _content_part_type(value: Any) -> str:
    if isinstance(value, Mapping):
        return str(value.get("type") or "").strip().lower()
    return str(getattr(value, "type", "") or "").strip().lower()


def _content_has_image(value: Any) -> bool:
    if isinstance(value, Mapping):
        return _content_part_type(value) in _IMAGE_CONTENT_TYPES
    return _content_part_type(value) in _IMAGE_CONTENT_TYPES


def _messages_have_image(messages: Any) -> bool:
    if not isinstance(messages, (list, tuple)):
        return False
    for message in messages:
        content = message.get("content") if isinstance(message, Mapping) else getattr(message, "content", None)
        if isinstance(content, (list, tuple)) and any(_content_has_image(part) for part in content):
            return True
    return False


def _responses_input_has_image(request_input: Any) -> bool:
    if not isinstance(request_input, (list, tuple)):
        return False
    for item in request_input:
        if not isinstance(item, Mapping):
            continue
        if _content_has_image(item):
            return True
        content = item.get("content")
        if isinstance(content, (list, tuple)) and any(_content_has_image(part) for part in content):
            return True
    return False


def request_contains_image(request_body: Any) -> bool:
    """Return whether a chat/Responses body contains an image input part."""
    if isinstance(request_body, Mapping):
        messages = request_body.get("messages")
        if _messages_have_image(messages):
            return True
        return _responses_input_has_image(request_body.get("input"))
    messages = getattr(request_body, "messages", None)
    if _messages_have_image(messages):
        return True
    return _responses_input_has_image(getattr(request_body, "input", None))


def detect_request_type(endpoint: Optional[str], request_body: Any) -> Optional[str]:
    normalized_endpoint = str(endpoint or "").strip().rstrip("/")
    if normalized_endpoint.endswith("/responses/compact"):
        return COMPACTION_REQUEST_TYPE

    if request_contains_image(request_body):
        return VISION_REQUEST_TYPE

    if isinstance(request_body, Mapping):
        request_input = request_body.get("input")
    else:
        request_input = getattr(request_body, "input", None)
    if not isinstance(request_input, list):
        return None

    for item in request_input:
        if (
            isinstance(item, Mapping)
            and item.get("type") == COMPACTION_TRIGGER_INPUT_TYPE
        ):
            return COMPACTION_REQUEST_TYPE
    return None
