# maas-server-sdk

MQTT 5.0 기반 **MaaS RPC 서비스(핸들러)** Python 패키지. 장비·엣지 런타임에서 `WMT/{ThingType}/{Service}/{VIN}/+/request`를 구독하고, MQTT 5.0 Properties로 응답·스트림을 발행한다.

규격·토픽·ACL·Reason Code:

- [TOPIC_AND_ACL_SPEC.md](../../../docs/TOPIC_AND_ACL_SPEC.md)
- [RPC_DESIGN.md](../../../docs/RPC_DESIGN.md)

---

## 요구 사항

- Python 3.10+
- `paho-mqtt` 2.x (MQTT 5.0)

인증서·TLS/WSS는 연결 옵션(`use_wss`, `port`)으로 선택한다.

---

## 설치

패키지 디렉터리(`sdk/python/server/`)에서:

```bash
pip install -e .[dev]
```

저장소 루트에서:

```bash
pip install -e sdk/python/server[dev]
```

Greengrass IPC 사용 시 추가 의존성 설치:

```bash
pip install -e sdk/python/server[aws]
```

---

## 패키지에서 가져오기

```python
from maas_server import MaasServer, RpcContext
from maas_server import topics  # 선택: 토픽 빌더·파서
```

---

## `MaasServer` 생성

담당 **ThingType**, **Service**, **VIN**, 브로커 **endpoint**를 고정한다. 이 조합에 맞는 WMT 요청만 처리한다.  
클라이언트 측 [`MaasClient`](../client/README.md)도 동일하게 생성자에 `thing_type` / `service` / `vin`을 맞추고 `call(action[, params])`로 호출하는 패턴과 대칭이다.

### 생성자 인자

| 인자 | 기본 | 설명 |
|------|------|------|
| `thing_type` | (필수) | 토픽 `{ThingType}` |
| `service_name` | (필수) | 토픽 `{Service}` |
| `vin` | (필수) | 토픽 `{VIN}` |
| `endpoint` | (필수) | 브로커 호스트 |
| `port` | `8883` | TCP TLS 등. 로컬 Mosquitto는 보통 `1883` + `use_wss=False` |
| `use_wss` | `False` | `True`면 WebSocket+TLS(경로 `/mqtt`) |
| `client_id` | `f"{service_name}-{vin}"` | MQTT ClientId |
| `route_key` | `"action"` | 페이로드에서 라우팅에 쓸 JSON 키. `None`이면 아래 **라우팅** 참고 |
| `exclusive_service` | `False` | `True`면 패턴 E 독점 세션 활성화. 비점유자 요청은 SDK가 0x8A 자동 거부 |
| `mode` | `'mqtt'` | `'mqtt'`: paho-mqtt(기본). `'greengrass'`: AWS Greengrass IPC(`pip install maas-server-sdk[aws]` 필요). |
| `logger` | — | 선택 로거 |

### 환경변수에서 생성

환경변수로 엔드포인트·VIN을 넘기는 배포용 팩토리:

```python
server = MaasServer.from_env(
    "CGU",
    "viss",
    vin_env="THING_VIN",
    endpoint_env="MQTT_ENDPOINT",
    route_key="action",
)
```

`THING_VIN`, `MQTT_ENDPOINT`가 없으면 `KeyError`이다.

### 예: TLS 브로커(TCP 8883)

```python
server = MaasServer(
    thing_type="CGU",
    service_name="viss",
    vin="VIN-123456",
    endpoint="mqtt.example.com",
    port=8883,
    use_wss=False,
    route_key="action",
)
```

### 예: 로컬 Mosquitto(TCP 1883)

```python
server = MaasServer(
    thing_type="CGU",
    service_name="viss",
    vin="VIN-123456",
    endpoint="127.0.0.1",
    port=1883,
    use_wss=False,
    client_id="example-viss-service",
    route_key="action",
)
```

저장소 예제: [examples/python/basic/echo_service.py](../../../examples/python/basic/echo_service.py).

---

## 라우팅: `route_key` · `@server.action` · `@server.default`

- **`route_key="action"`(기본):** 요청 JSON에서 `action` 필드를 읽어 `@server.action("이름")`과 매칭한다. [TOPIC_AND_ACL_SPEC.md](../../../docs/TOPIC_AND_ACL_SPEC.md) §5와 동일.
- **다른 키:** `route_key="method"` 등으로 바꿀 수 있다. 클라이언트·문서와 **같은 키**를 써야 한다.
- **`route_key=None`:** `@server.action`은 **사용할 수 없다**. `@server.default` **하나만** 등록한다. 페이로드에서 라우팅 필드를 제거하지 않고 **전체 dict**가 `RpcContext.payload`로 전달된다.
- **기본 핸들러(선택):** `route_key`가 문자열일 때, 해당 필드가 없거나 빈 값이면 `@server.default`가 호출된다.

```python
@server.action("get")
def get_datapoint(ctx: RpcContext):
    return {"value": 42, "path": ctx.payload.get("path")}

@server.default()
def fallback(ctx: RpcContext):
    return {"error": "unknown route"}
```

---

## RPC 핸들러: `@server.action`

```python
@server.action("get")
def get_datapoint(ctx: RpcContext):
    return {"path": ctx.payload.get("path"), "value": 42.0}
```

데코레이터 옵션:

| 옵션 | 의미 |
|------|------|
| `subscription=True` | 핸들러는 동기/비동기 **제너레이터**. 청크(`is_EOF=false`)와 완료 신호(`is_EOF=true`) 모두 `WMO/.../response` 토픽으로 발행 |
| `qos=0` | 스트림 청크 발행 QoS (`subscription=True` 시) |

패턴 E(독점 세션)는 `MaasServer(exclusive_service=True)` 로 활성화하고, 핸들러 안에서 `server.acquire_session(ctx.client_id)` / `server.release_session(ctx.client_id)` 를 호출한다. 세션 점유 중 다른 클라이언트의 요청은 SDK가 0x8A로 자동 거부한다.

`RpcContext` 주요 필드: `thing_type`, `service`, `action`(라우트 라벨), `vin`, `client_id`, `payload`(라우팅 키 제거 후 나머지), `correlation_id`, `response_topic`, `user_props`.

---

## 임의 구독·발행: `@server.subscribe`, `publish`

```python
@server.subscribe("shadow/update/#")
def on_shadow(topic: str, payload: bytes) -> None:
    ...

server.run()  # 블로킹
# 실행 중에만:
server.publish("some/topic", {"a": 1}, qos=1)
```

`run()`이 돌고 있는 동안에만 `publish`를 호출할 수 있다. 종료는 `KeyboardInterrupt` 또는 다른 스레드에서 `server.stop()`.

---

## 실행

```python
server.run()   # asyncio.run 기반, 블로킹
```

---

## Greengrass 배포 가이드

### mode='greengrass' 사용

```python
server = MaasServer(
    thing_type="CGU",
    service_name="viss",
    # vin은 생략 가능 — connect() 후 AWS_IOT_THING_NAME 환경변수에서 자동 취득
    mode="greengrass",
)
```

- `endpoint`와 `vin`은 생략 가능하다. `connect()` 호출 후 Greengrass 런타임이 설정하는 `AWS_IOT_THING_NAME` 환경변수에서 VIN을 취득한다.
- `awsiotsdk`가 필요하다: `pip install maas-server-sdk[aws]`.

### Greengrass 환경에서 from_env() 사용

```python
# Greengrass 환경
server = MaasServer.from_env(
    "CGU",
    "viss",
    mode="greengrass",
)
```

### Greengrass recipe accessControl

Greengrass 컴포넌트 레시피에서 IoT Core 토픽 권한을 설정할 때:

```python
perms = MaasServer.get_required_iot_core_permissions(
    thing_type="CGU",
    service_name="viss",
    vin="VIN-123456",
)
# perms를 recipe의 accessControl 섹션에 삽입
```

### 커스텀 어댑터

`MqttClientAdapter` Protocol을 직접 구현하려면:

```python
from maas_server._adapter import MqttClientAdapter, MqttProperties
```

---

## 문서

- [SDK 요구사양서](../../../docs/spec/SDK%20요구사양서.md)
- [SDK 상세설계사양서](../../../docs/spec/SDK%20상세설계사양서.md)
- [SDK 개요](../../../sdk/README.md)

---

## 라이선스

Proprietary (프로젝트 정책에 따름).
