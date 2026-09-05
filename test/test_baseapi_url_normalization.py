import asyncio
from io import BytesIO
import os
import sys

import pytest
import httpx

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import ImageGenerationRequest, ImageEditRequest
from core.request import get_dalle_payload
from core.response import _build_multipart_content
from core.utils import BaseAPI, get_engine


@pytest.mark.parametrize(
    ("source_url", "expected_image_url", "expected_image_edit_url"),
    [
        (
            "https://oaix.fugue.pro/v1/responses",
            "https://oaix.fugue.pro/v1/images/generations",
            "https://oaix.fugue.pro/v1/images/edits",
        ),
        (
            "https://oaix.fugue.pro/v1/responses/compact",
            "https://oaix.fugue.pro/v1/images/generations",
            "https://oaix.fugue.pro/v1/images/edits",
        ),
        (
            "https://oaix.fugue.pro/v1/images/generations",
            "https://oaix.fugue.pro/v1/images/generations",
            "https://oaix.fugue.pro/v1/images/edits",
        ),
        (
            "https://oaix.fugue.pro/v1/images/edits",
            "https://oaix.fugue.pro/v1/images/generations",
            "https://oaix.fugue.pro/v1/images/edits",
        ),
    ],
)
def test_base_api_normalizes_image_urls(source_url, expected_image_url, expected_image_edit_url):
    base_api = BaseAPI(source_url)

    assert base_api.image_url == expected_image_url
    assert base_api.image_edit_url == expected_image_edit_url


def test_get_dalle_payload_uses_normalized_images_endpoint_for_responses_provider():
    provider = {
        "provider": "fugue-codex",
        "base_url": "https://oaix.fugue.pro/v1/responses",
        "api": "change-me",
        "engine": "codex",
        "model": ["gpt-image-2"],
    }
    request = ImageGenerationRequest(model="gpt-image-2", prompt="test prompt")

    url, headers, payload = asyncio.run(get_dalle_payload(request, "dalle", provider, api_key="change-me"))

    assert url == "https://oaix.fugue.pro/v1/images/generations"
    assert headers == {
        "Content-Type": "application/json",
        "Authorization": "Bearer change-me",
    }
    assert payload == {
        "model": "gpt-image-2",
        "prompt": "test prompt",
    }


def test_get_dalle_payload_uses_normalized_images_edits_endpoint_for_responses_provider():
    provider = {
        "provider": "fugue-codex",
        "base_url": "https://oaix.fugue.pro/v1/responses",
        "api": "change-me",
        "engine": "codex",
        "model": ["gpt-image-2"],
    }
    request = ImageEditRequest(
        model="gpt-image-2",
        prompt="edit this",
        images=[{"image_url": "data:image/png;base64,abc"}],
        mask={"image_url": "data:image/png;base64,mask"},
        response_format="b64_json",
    )

    url, headers, payload = asyncio.run(
        get_dalle_payload(request, "dalle", provider, api_key="change-me", endpoint="/v1/images/edits")
    )

    assert url == "https://oaix.fugue.pro/v1/images/edits"
    assert headers == {
        "Content-Type": "application/json",
        "Authorization": "Bearer change-me",
    }
    assert payload == {
        "model": "gpt-image-2",
        "prompt": "edit this",
        "images": [{"image_url": "data:image/png;base64,abc"}],
        "mask": {"image_url": "data:image/png;base64,mask"},
        "response_format": "b64_json",
    }


def test_get_dalle_payload_preserves_images_edits_multipart_without_json_content_type():
    provider = {
        "provider": "fugue-codex",
        "base_url": "https://oaix.fugue.pro/v1/responses",
        "api": "change-me",
        "engine": "codex",
        "model": [{"gpt-image-2": "image-alias"}],
    }
    image_file = BytesIO(b"image-bytes")
    request = ImageEditRequest(
        model="image-alias",
        prompt="edit this",
        multipart_data=[("prompt", "edit this"), ("model", "image-alias"), ("size", "1024x1024")],
        multipart_files=[("image", ("image.png", image_file, "image/png"))],
    )

    url, headers, payload = asyncio.run(
        get_dalle_payload(request, "dalle", provider, api_key="change-me", endpoint="/v1/images/edits")
    )

    assert url == "https://oaix.fugue.pro/v1/images/edits"
    assert headers == {"Authorization": "Bearer change-me"}
    assert payload["__multipart_data__"] == [
        ("prompt", "edit this"),
        ("size", "1024x1024"),
        ("model", "gpt-image-2"),
    ]
    assert payload["__multipart_files__"] == [("image", ("image.png", image_file, "image/png"))]


def test_multipart_content_builder_avoids_httpx_sync_files_stream():
    headers, content = _build_multipart_content(
        {"Authorization": "Bearer change-me", "Content-Type": "application/json"},
        [("prompt", "edit this"), ("model", "gpt-image-2")],
        [("image", ("image.png", BytesIO(b"image-bytes"), "image/png"))],
    )

    assert headers["Authorization"] == "Bearer change-me"
    assert headers["Content-Type"].startswith("multipart/form-data; boundary=")
    assert b'name="prompt"' in content
    assert b'name="image"; filename="image.png"' in content
    assert b"image-bytes" in content

    client = httpx.AsyncClient()
    try:
        request = client.build_request("POST", "https://example.com/v1/images/edits", headers=headers, content=content)
        assert hasattr(request.stream, "__aiter__")
    finally:
        asyncio.run(client.aclose())


def test_get_engine_routes_images_edits_to_dalle_without_forcing_stream_mode():
    provider = {
        "provider": "fugue-codex",
        "base_url": "https://oaix.fugue.pro/v1/responses",
        "engine": "codex",
        "model": ["gpt-image-2"],
    }

    engine, stream = get_engine(provider, endpoint="/v1/images/edits", original_model="gpt-image-2")

    assert engine == "dalle"
    assert stream is None
