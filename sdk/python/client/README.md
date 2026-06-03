# maas-client-sdk

MQTT 5.0 기반 **MaaS RPC 클라이언트** Python 패키지. MQTT 브로커(WSS+TLS 또는 TCP)에 연결해 `WMT` 요청을 발행하고 `WMO` 응답·이벤트를 수신한다. 별도 Envelope 없이 MQTT 5.0 `Response Topic`, `Correlation Data`, `User Properties`를 사용한다.

규격·토픽·ACL·Reason Code는 다음 문서가 단일 출처다.

- [TOPIC_AND_ACL_SPEC.md](../../../docs/TOPIC_AND_ACL_SPEC.md)
- [RPC_DESIGN.md](../../../docs/RPC_DESIGN.md)

---

## 요구 사항

- Python 3.10+
- `paho-mqtt` 2.x (MQTT 5.0)

---

## 설치

패키지 디렉터리(`sdk/python/client/`)에서:

```bash
pip install -e .[dev]   # pytest 등 개발 의존성 포함
```

저장소 루트에서 editable 설치:

```bash
pip install -e sdk/python/client[dev]
```

---

## 패키지에서 가져오기

```python
from maas_client import (
    MaasClient,           # 동기(기본)
    MaasClientAsync,      # asyncio
    HttpTokenSource,      # AT 등 HTTP로 토큰 획득 (선택)
    RpcResponse,
    StreamEvent,
    Message,
    topics,               # WMT/WMO 토픽 빌더·파서
)
from maas_client.exceptions import (
    ConnectionError,
    RpcTimeoutError,
    RpcServerError,
    NotAuthorizedError,
    ServerBusyError,
    SubscriptionCancelledError,
    ServerOfflineError,
)
```

---

## 최소 예제 (권장): 생성자 바인딩 + `call`

대상 `thing_type`, `service`, `vin`을 생성자에 고정하면 **`call(action[, params], ...)`** 만으로 RPC를 보낸다. ([`MaasServer`](../../../sdk/python/server/README.md)의 ThingType/Service/VIN 고정과 대칭.)  
`params`는 RPC 인자(곧 MQTT 메시지 **본문 JSON**에 실리는 필드)이며, 응답은 여전히 `RpcResponse.payload`로 받는다.

```python
client = MaasClient(
    endpoint="mqtt.example.com",
    client_id="my-app-unique-id",
    thing_type="CGU",
    service="viss",
    vin="VIN-123456",
    token_provider=lambda: fetch_jwt(),  # 또는 None (로컬 무인증)
)
client.connect()
r = client.call("get", {"path": "Vehicle.Speed"})
print(r.payload)
client.disconnect()
```

비동기:

```python
async with MaasClientAsync(
    endpoint="mqtt.example.com",
    client_id="my-app-unique-id",
    thing_type="CGU",
    service="viss",
    vin="VIN-123456",
    token_provider=get_jwt,
) as client:
    r = await client.call("get", {"path": "Vehicle.Speed"})
```

---

## 연결

### WSS + TLS (기본)

`endpoint`는 **호스트명만**(포트 제외). 기본 포트는 **443**.

`connect()` 할 때마다 `token_provider`가 있으면 **한 번 호출**되어 그 결과가 MQTT username 등으로 쓰인다(재연결·토큰 갱신에 유리).

```python
client = MaasClient(
    endpoint="mqtt.example.com",
    client_id="my-app-unique-id",
    thing_type="CGU",
    service="viss",
    vin="VIN-123456",
    token_provider=lambda: fetch_jwt(),
)
client.connect()
client.disconnect()
```

컨텍스트 매니저:

```python
with MaasClient(
    endpoint="...",
    client_id="...",
    thing_type="CGU",
    service="viss",
    vin="VIN-1",
) as client:
    ...
```

### 로컬 브로커 (TCP, 무암호화)

Mosquitto 등 **일반 MQTT TCP**에 붙을 때는 `use_wss=False`이다. `port`를 생략하면 **1883**이 쓰인다.

```python
client = MaasClient(
    endpoint="127.0.0.1",
    client_id="local-demo",
    token_provider=None,
    use_wss=False,
    thing_type="CGU",
    service="viss",
    vin="VIN-123456",
)
```

### 생성자 인자 요약

| 인자 | 설명 |
|------|------|
| `endpoint` | 브로커 호스트 |
| `client_id` | MQTT ClientId. `WMT/.../{client_id}/request`, `WMO/.../{client_id}/response`에 사용 |
| `token_provider` | `None`이면 무인증. 있으면 **매 `connect()`** 시 호출해 JWT 등 문자열 사용 |
| `port` | `None`이면 WSS→443, TCP→1883 |
| `use_wss` | `True`: WebSocket+TLS. `False`: TCP(로컬 등) |
| `thing_type` | (선택) 바인딩 시 토픽 ThingType. `service`, `vin`과 **세 값 모두** 지정 |
| `service` | (선택) 바인딩 시 서비스 이름 |
| `vin` | (선택) 바인딩 시 대상 VIN |
| `logger` | 선택 로거 |

`thing_type` / `service` / `vin`은 **모두 생략하거나 모두 지정**해야 한다.

`MaasClientAsync`는 동일한 생성자 인자를 받는다.

---

## 고급: 인증·토큰 (`token_provider`, `HttpTokenSource`)

앱에서 JWT 문자열을 직접 다루지 않으려면, AT(토큰 발급) HTTP 응답이 `{"access_token": "..."} ` 형태일 때 `HttpTokenSource`를 `token_provider`에 넘길 수 있다. URL·헤더·본문은 배포 계약에 맞게 설정한다(다른 JSON 스키마면 서브클래스에서 파싱을 오버라이드).

```python
from maas_client import MaasClient, HttpTokenSource

issuer = HttpTokenSource(
    "https://at.example.com/oauth/token",
    payload=b'{"grant_type":"client_credentials"}',
    headers={"Content-Type": "application/json"},
)
client = MaasClient(
    endpoint="mqtt.example.com",
    client_id="app-1",
    thing_type="CGU",
    service="viss",
    vin="VIN-1",
    token_provider=issuer,
)
```

임의의 `Callable[[], str]`도 그대로 사용 가능하다.

---

## RPC: 단일 응답 `call`

`call` 은 **위치 인자 개수**로 두 가지 모드를 구분한다.

1. **생성자 바인딩** (`thing_type`, `service`, `vin` 모두 지정):  
   `call(action)` 또는 `call(action, params_dict)` 또는 `call(action, params=...)`.
2. **명시 라우팅** (플릿·멀티 VIN):  
   `call(thing_type, service, action, vin, params=...)` 또는  
   `call(thing_type, service, action, vin, params_dict)` (다섯 번째 위치).

`qos`, `timeout`, `expiry` 등은 항상 **키워드 인자**로 넘긴다.

**QoS 1과 Message Expiry:** 기본 RPC는 QoS 1이다. 이 경우 SDK는 MQTT 5 `Message Expiry Interval`을 **응답 대기 `timeout`(초)과 같게** 자동 설정한다(소수 초는 올림, 최소 1초). 브로커가 오래된 요청을 무기한 전달하지 않도록, 클라이언트 타임아웃과 맞춘다. 이때 `expiry=` 인자는 **무시**된다.

**QoS 0과 `expiry`:** QoS 0은 수신 세션이 없으면 메시지가 바로 사라지는 것이 일반적이라, 브로커 `Message Expiry`의 실효는 **대부분 제한적**이다. SDK는 호환을 위해 `qos=0`일 때만 `expiry=`를 PUBLISH에 넣을 수 있다(미지정이면 속성 생략). 시한성 제어·패턴 D는 **`qos=1` + `timeout`(자동 Expiry)** 를 권장한다(`docs/RPC_DESIGN.md` 패턴 D).

**바인딩 예:**

```python
r = client.call(
    "get",
    {"path": "Vehicle.Speed"},
    qos=1,
    timeout=10.0,
    # QoS 1: Message Expiry ≈ 10초(자동). expiry 인자 불필요·무시.
)
print(r.payload, r.reason_code)
```

**명시 라우팅 예:**

```python
r = client.call(
    "CGU",
    "viss",
    "get",
    "VIN-123456",
    params={"path": "Vehicle.Speed"},
    qos=1,
    timeout=10.0,
)
```

- **반환:** `RpcResponse` — 응답 본문 `payload`, `reason_code`, `correlation_id`
- **예외:** `RpcTimeoutError`, `RpcServerError`(및 Reason에 따른 하위 타입), 인자 조합 오류 시 `TypeError` / 바인딩 단축인데 생성자에 라우팅이 없으면 `ValueError`

비동기: `await client.call("get", {...})` 등 동일 규칙.

---

## RPC: 스트리밍 `stream`

`stream` 도 `call` 과 같은 **1·2개(바인딩)** vs **4·5개(명시)** 위치 인자 규칙을 따른다.

서버가 청크(`is_EOF=false`)와 완료 신호(`is_EOF=true`)를 모두 `WMO/.../response` 토픽으로 보내는 패턴([RPC_DESIGN.md](../../../docs/RPC_DESIGN.md) 패턴 C).

**바인딩** (`MaasClient`):

```python
for ev in client.stream("can_log", {}, qos=1, chunk_timeout=60.0):
    if not ev.is_eof:
        print(ev.payload)
```

**명시 라우팅:**

```python
for ev in client.stream(
    "CGU",
    "diagnostics",
    "can_log",
    "VIN-123456",
    params={},
    qos=1,
    chunk_timeout=60.0,
):
    if not ev.is_eof:
        print(ev.payload)
```

**비동기** (`MaasClientAsync`): `async for ev in client.stream("can_log", {}):` 등.

---

## RPC: 독점 세션 `exclusive_session` (패턴 E)

진입 시 `acquire_action`, 종료 시 `release_action` RPC를 자동 호출한다. 기본 액션 이름은 `session_start` / `session_stop`이다.

**바인딩** (인자 없음):

```python
with client.exclusive_session() as session:
    r = session.call("ecu_reset", params={})
```

**명시 라우팅:**

```python
with client.exclusive_session("CGU", "uds", "VIN-123456") as session:
    r = session.call("ecu_reset", params={})
```

**비동기:**

```python
async with client.exclusive_session() as session:
    r = await session.call(action="ecu_reset", params={})
```

---

## 임의 Pub/Sub

RPC와 별도로 임의 토픽에 발행·구독할 수 있다. 구독 콜백은 `(Message) -> None` 형태이며, `Message`는 `topic`, `payload`(bytes), `qos`, `user_properties`를 담는다.

```python
def on_msg(m: Message) -> None:
    print(m.topic, m.payload)

client.subscribe("custom/topic/#", on_msg, qos=1)
client.publish("custom/topic/x", {"k": "v"}, qos=0)
client.unsubscribe("custom/topic/#")
```

`MaasClientAsync`에서는 `await client.subscribe(...)`, `await client.publish(...)`이다.

---

## 기타 API

| 항목 | 설명 |
|------|------|
| `client.client_id` | 연결에 쓰인 ClientId |
| `client.is_connected` | 연결 여부 |
| `maas_client.topics` | `build_request`, `build_response_wildcard` 등 토픽 유틸 |

---

## 로컬 검증 예제

저장소의 [examples/python/basic/call_client.py](../../../examples/python/basic/call_client.py) — `use_wss=False`로 Mosquitto와 `maas-server-sdk` 서비스를 연동한다.

---

## 문서

- [RPC 설계](../../../docs/RPC_DESIGN.md)
- [토픽·ACL](../../../docs/TOPIC_AND_ACL_SPEC.md)
- [SDK 개요](../../../sdk/README.md)

---

## 라이선스

Proprietary (프로젝트 정책에 따름).
