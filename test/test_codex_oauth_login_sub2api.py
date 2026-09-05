import base64
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "test" / "codex_oauth_login.py"
    spec = importlib.util.spec_from_file_location("codex_oauth_login_script", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _jwt(payload: dict) -> str:
    header = {"alg": "none", "typ": "JWT"}

    def encode(value: dict) -> str:
        raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{encode(header)}.{encode(payload)}.signature"


def test_build_sub2api_payload_maps_claims_and_defaults() -> None:
    module = _load_module()
    issued_at = 2_000_000_000
    exported_at = datetime.fromtimestamp(issued_at + 100, tz=timezone.utc)
    local_tz = timezone(timedelta(hours=8))

    access_token = _jwt(
        {
            "iat": issued_at,
            "exp": issued_at + 3600,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct-123",
                "chatgpt_user_id": "user-456",
                "chatgpt_plan_type": "plus",
            },
            "https://api.openai.com/profile": {
                "email": "user@example.com",
            },
        }
    )
    id_token = _jwt(
        {
            "iat": issued_at,
            "email": "user@example.com",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct-123",
                "organizations": [{"id": "org-789"}],
            },
        }
    )
    source = {
        "access_token": access_token,
        "expires_in": 7200,
        "id_token": id_token,
        "refresh_token": "refresh-abc",
    }

    converted = module.build_sub2api_payload(source, exported_at=exported_at, local_tz=local_tz)
    account = converted["accounts"][0]
    credentials = account["credentials"]
    extra = account["extra"]

    assert converted["type"] == "sub2api-data"
    assert converted["version"] == 1
    assert converted["exported_at"] == "2033-05-18T03:35:00Z"
    assert converted["proxies"] == []

    assert account["name"] == "user@example.com"
    assert account["platform"] == "openai"
    assert account["type"] == "oauth"
    assert account["concurrency"] == 10
    assert account["priority"] == 1
    assert account["rate_multiplier"] == 1
    assert account["expires_at"] == issued_at + 3600
    assert account["auto_pause_on_expired"] is True

    assert credentials["access_token"] == access_token
    assert credentials["email"] == "user@example.com"
    assert credentials["chatgpt_account_id"] == "acct-123"
    assert credentials["chatgpt_user_id"] == "user-456"
    assert credentials["expires_at"] == issued_at + 3600
    assert credentials["expires_in"] == 3500
    assert credentials["id_token"] == id_token
    assert credentials["organization_id"] == "org-789"
    assert credentials["refresh_token"] == "refresh-abc"
    assert credentials["plan_type"] == "plus"

    assert extra["email"] == "user@example.com"
    assert extra["email_key"] == "user@example.com"
    assert extra["last_refresh"] == datetime.fromtimestamp(issued_at, tz=timezone.utc).astimezone(local_tz).isoformat()
    assert extra["privacy_mode"] == "training_set_cf_blocked"
    assert extra["privacy_checked_at"] == exported_at.astimezone(local_tz).isoformat()
