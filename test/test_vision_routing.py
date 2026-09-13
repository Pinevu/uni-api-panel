import asyncio
from types import SimpleNamespace

from uni_api.upstream.policies import ProviderErrorClassifier, RetryPolicy
from uni_api.routing.request_types import (
    VISION_REQUEST_TYPE,
    detect_request_type,
    request_contains_image,
)
from uni_api.routing.core import get_right_order_providers


def test_detect_request_type_detects_chat_and_responses_images():
    chat = {
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": "look"}, {"type": "image_url", "image_url": {"url": "data:image/png;base64,x"}}],
        }],
    }
    responses = {
        "input": [{
            "role": "user",
            "content": [{"type": "input_text", "text": "look"}, {"type": "input_image", "image_url": "data:image/png;base64,x"}],
        }],
    }
    assert request_contains_image(chat)
    assert request_contains_image(responses)
    assert detect_request_type("/v1/chat/completions", chat) == VISION_REQUEST_TYPE
    assert detect_request_type("/v1/responses", responses) == VISION_REQUEST_TYPE


def test_get_right_order_providers_excludes_provider_with_image_false():
    config = {
        "providers": [
            {"provider": "text-only", "base_url": "https://text.example/v1", "api": "text-key", "model": ["grok-4.5"], "image": False},
            {"provider": "vision", "base_url": "https://vision.example/v1", "api": "vision-key", "model": ["grok-4.5"]},
        ],
        "api_keys": [{"api": "sk-test", "model": ["grok-4.5"]}],
    }

    async def run():
        providers = await get_right_order_providers(
            "grok-4.5", config, 0, "fixed_priority", ["sk-test"], {"sk-test": ["grok-4.5"]}, request_type=VISION_REQUEST_TYPE,
        )
        assert [item["provider"] for item in providers] == ["vision"]

    asyncio.run(run())


def test_text_only_image_error_is_retryable_failover():
    def safe_get(data, *keys, default=None):
        current = data
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key, default)
        return current

    classifier = ProviderErrorClassifier(safe_get)
    retry_policy = RetryPolicy(classifier, lambda *_args: ("gpt", None))
    details = {"error": {"message": "Model only supports text input; received unsupported content type 'image_url'."}}
    assert classifier.is_unsupported_image_input_error(400, details)
    assert classifier.remap_status_code(400, str(details)) == 502
    assert retry_policy.should_retry(True, 502, {"base_url": "https://text.example/v1"}, error_message=str(details))
