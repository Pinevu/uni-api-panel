import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.request import prepare_request_payload


async def test_gemini_chat_service_tier_maps_to_native_service_tier():
    provider = {
        "provider": "gemini",
        "base_url": "https://generativelanguage.googleapis.com/v1beta",
        "api": "test-key",
        "model": ["gemini-2.5-flash"],
    }

    request_data = {
        "model": "gemini-2.5-flash",
        "messages": [{"role": "user", "content": "hello"}],
        "service_tier": "priority",
        "stream": False,
    }

    _, _, payload, engine = await prepare_request_payload(provider, request_data)

    assert engine == "gemini"
    assert payload["serviceTier"] == "PRIORITY"
    assert "service_tier" not in payload


async def test_gemini_chat_default_service_tier_maps_to_standard():
    provider = {
        "provider": "gemini",
        "base_url": "https://generativelanguage.googleapis.com/v1beta",
        "api": "test-key",
        "model": ["gemini-2.5-flash"],
    }

    request_data = {
        "model": "gemini-2.5-flash",
        "messages": [{"role": "user", "content": "hello"}],
        "service_tier": "default",
        "stream": False,
    }

    _, _, payload, _ = await prepare_request_payload(provider, request_data)

    assert payload["serviceTier"] == "STANDARD"


if __name__ == "__main__":
    asyncio.run(test_gemini_chat_service_tier_maps_to_native_service_tier())
    asyncio.run(test_gemini_chat_default_service_tier_maps_to_standard())
