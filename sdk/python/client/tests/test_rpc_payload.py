"""RPC 요청 본문 빌더(_build_request_payload) 동작 검증."""

import json

from maas_client._rpc import _build_request_payload


def test_action_overrides_params_dict() -> None:
    raw = _build_request_payload("get", {"path": "/x", "action": "old"})
    data = json.loads(raw.decode("utf-8"))
    assert data["action"] == "get"
    assert data["path"] == "/x"


def test_none_params_only_action() -> None:
    raw = _build_request_payload("ping", None)
    data = json.loads(raw.decode("utf-8"))
    assert data == {"action": "ping"}


def test_action_key_is_first_in_json_object() -> None:
    """직렬화 시 action 필드가 JSON 객체에서 맨 앞에 온다."""
    raw = _build_request_payload("get", {"path": "/x"})
    text = raw.decode("utf-8")
    assert text.startswith('{"action":')
