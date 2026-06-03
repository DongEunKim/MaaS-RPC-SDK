# SDK 요구사양서

> 문서 유형: SDK 요구사양서
> 독자: PM, 아키텍트, SDK 통합 개발자

토픽·페이로드·Reason Code 규범은 [TOPIC_AND_ACL_SPEC.md](../TOPIC_AND_ACL_SPEC.md), RPC 패턴 정의는 [RPC_DESIGN.md](../RPC_DESIGN.md)를 단일 출처로 한다.

---

## 1. 개요

MaaS RPC SDK는 MQTT 5.0 브로커를 통해 클라이언트와 엣지 서비스 사이의 RPC를 캡슐화하는 두 Python 패키지로 구성된다.

| 패키지 | 역할 |
|--------|------|
| `maas-client-sdk` | RPC 호출 + pub/sub. 웹 앱, 백오피스 등에서 사용 |
| `maas-server-sdk` | RPC 핸들러 + pub/sub. 엣지 서비스에서 사용 |

---

## 2. 적용 범위

본 문서는 두 SDK의 **기능 요구사항**과 **비기능 요구사항**을 정의한다. 구체적인 내부 구현 구조는 [SDK 상세설계사양서.md](SDK%20%EC%83%81%EC%84%B8%EC%84%A4%EA%B3%84%EC%82%AC%EC%96%91%EC%84%9C.md)에서 다룬다.

---

## 3. 용어 정의

| 용어 | 정의 |
|------|------|
| `thing_type` | 장비 타입 분류 (예: CGU, SDM) |
| `service` | 서비스 이름 (예: viss, diagnostics) |
| `vin` | 장비 식별자 (Vehicle Identification Number) |
| `client_id` | MQTT 클라이언트 식별자. 응답 토픽 라우팅에 사용 |
| `correlation_id` | 요청-응답 매핑용 UUID v4 (bytes). SDK가 자동 생성 |
| `route_key` | 페이로드에서 핸들러 라우팅에 사용할 JSON 키 이름 (기본 `"action"`) |
| `RpcContext` | 서버 핸들러가 수신하는 요청 컨텍스트 객체 |

---

## 4. maas-server-sdk 기능 요구사항

### 4.1 필수 기능 목록

| 기능 | 설명 |
|------|------|
| RPC 수신 및 라우팅 | WMT 토픽의 요청을 수신하고 `route_key` 필드로 핸들러를 선택하여 실행 |
| 응답 자동 발행 | 핸들러 반환값을 response 토픽에 자동 발행. Correlation Data, reason_code 자동 처리 |
| 스트리밍 지원 | generator / async generator 핸들러로 청크를 순차 발행. EOF 자동 처리 |
| 독점 세션 | `exclusive_service=True` 서비스 단위 단일 세션. `acquire_session()`/`release_session()` 으로 제어 |
| pub/sub 확장 | `@server.subscribe("패턴")`으로 임의 토픽 핸들러 등록 |
| 전송 계층 교체 | `mode="mqtt"` (paho) 또는 `mode="greengrass"` (Greengrass IPC) 선택 |
| 예외 처리 | 핸들러 미처리 예외 → `reason_code=0x80`으로 클라이언트에 전달. 서버 크래시 없음 |

### 4.2 핸들러 등록 API

#### `@server.action(action_name, *, subscription, qos)`

페이로드의 `route_key` 필드 값이 `action_name`과 일치할 때 호출되는 핸들러를 등록한다.

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `action_name` | `str` | 필수 | 페이로드 `route_key` 필드와 일치해야 하는 값 |
| `subscription` | `bool` | `False` | True이면 generator / async generator 스트리밍 핸들러 |
| `qos` | `int` | `0` | 스트림 청크 발행 QoS (`subscription=True` 시) |

#### `@server.default(*, subscription, qos)`

- `route_key`가 문자열일 때: `route_key` 필드가 없거나 빈 값이면 이 핸들러가 호출된다
- `route_key=None`일 때: 모든 RPC 요청이 이 핸들러로 전달되며 페이로드를 분해하지 않는다. `@server.action`과 병용 불가

#### `@server.subscribe(topic_pattern)`

임의 토픽 구독. 핸들러 시그니처: `def handler(topic: str, payload: bytes)`

### 4.3 RpcContext 명세

핸들러가 수신하는 요청 컨텍스트.

| 필드 | 타입 | 설명 |
|------|------|------|
| `thing_type` | `str` | 요청 토픽의 ThingType |
| `service` | `str` | 요청 토픽의 Service |
| `action` | `str` | 라우팅에 사용된 action 값 |
| `vin` | `str` | 요청 토픽의 VIN |
| `client_id` | `str` | 요청 토픽의 ClientId |
| `payload` | `dict` | JSON 페이로드에서 `route_key` 필드를 제거한 나머지 |
| `correlation_id` | `bytes | None` | Correlation Data |
| `response_topic` | `str | None` | Response Topic |
| `user_props` | `dict[str, str]` | MQTT User Property 딕셔너리 |

### 4.4 스트리밍 지원

`subscription=True` 핸들러는 동기 generator 또는 async generator이어야 한다.

```python
@server.action("can_log", subscription=True)
def stream_log(ctx: RpcContext):
    for chunk in read_chunks():
        yield chunk           # is_EOF=false로 발행
# generator 종료 → SDK가 자동으로 is_EOF=true 발행
```

async generator도 지원한다.

```python
@server.action("can_log", subscription=True)
async def stream_log_async(ctx: RpcContext):
    async for chunk in async_read_chunks():
        yield chunk
```

### 4.5 독점 세션 지원 (패턴 E)

`MaasServer(exclusive_service=True)` 로 활성화하고, 핸들러 안에서 세션을 획득·해제한다.
세션 점유 중 다른 클라이언트의 모든 요청은 SDK가 `0x8A`(ServerBusy)로 자동 거부한다.

```python
server = MaasServer(..., exclusive_service=True)

@server.action("session_start")
def start(ctx):
    if server.acquire_session(ctx.client_id) is None:
        raise HandlerError("점유 중", reason_code=0x8A)
    return {"ok": True}

@server.action("read_data")     # 점유자만 호출 가능 (SDK 자동 게이팅)
def read(ctx): return {"data": ...}

@server.action("session_stop")
def stop(ctx):
    server.release_session(ctx.client_id)
    return {"ok": True}
```

점유 클라이언트가 비정상 단절되면 LWT(offline) 토픽을 통해 세션이 강제 해제된다.

### 4.6 전송 모드 (mode="mqtt" / mode="greengrass")

| mode | 필수 파라미터 | 선택 파라미터 |
|------|-------------|-------------|
| `"mqtt"` (기본) | `endpoint`, `vin` | `port`, `use_wss` |
| `"greengrass"` | 없음 | `vin` (없으면 `AWS_IOT_THING_NAME` 환경변수로 자동 취득) |

```python
# CCU — pure MQTT
server = MaasServer(
    thing_type="CGU", service_name="viss", vin="VIN-001",
    endpoint="emqx.local", port=1883, use_wss=False,
)

# CGU — Greengrass IPC
server = MaasServer(
    thing_type="CGU", service_name="viss",
    mode="greengrass",
)
```

### 4.7 IoT Core 권한 패턴 생성 헬퍼

Greengrass component recipe의 `accessControl` 섹션에 필요한 IoT Core 권한 패턴(요청·LWT 구독, 응답·heartbeat 발행)을 정적 메서드로 생성한다. 권한 패턴의 규범은 [TOPIC_AND_ACL_SPEC.md](../TOPIC_AND_ACL_SPEC.md) §9를 단일 출처로 한다.

```python
acl = MaasServer.get_required_iot_core_permissions("CGU", "viss", "VIN-123456")
```

---

## 5. maas-client-sdk 기능 요구사항

### 5.1 필수 기능 목록

| 기능 | 설명 |
|------|------|
| 단일 RPC 호출 | `call()`. Correlation Data 자동 관리. timeout 내 응답 대기 |
| 스트리밍 RPC | `stream()`. async for / 동기 for로 청크 수신 |
| 독점 세션 | `exclusive_session()`. 컨텍스트 매니저로 Lock 획득·해제 자동화 |
| pub/sub | `publish()`, `subscribe()`, `unsubscribe()`로 임의 토픽 접근 |
| 동기·비동기 지원 | `MaasClient` (동기 Facade), `MaasClientAsync` (asyncio) |
| 토큰 인증 | `token_provider` 콜백으로 JWT 등 단기 토큰 자동 주입 |

### 5.2 call() API 명세

단일 RPC 호출. QoS 1이면 `Message Expiry Interval`을 `timeout`과 자동 동기화한다.

**이중 호출 규칙:**

| 형식 | 전제 |
|------|------|
| `call(action[, params])` | 생성자에 `thing_type`, `service`, `vin` 모두 지정 |
| `call(thing_type, service, action, vin[, params])` | 생성자 바인딩 불필요 |

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `qos` | `int` | `1` | MQTT QoS (0 또는 1) |
| `timeout` | `float` | `10.0` | 응답 대기 타임아웃(초). QoS 1이면 Expiry도 이 값에서 유도 |
| `expiry` | `int \| None` | `None` | QoS 0에서만 PUBLISH Expiry 적용. QoS 1에서는 무시 |

반환: `RpcResponse` (`.payload`, `.reason_code`, `.correlation_id`)

예외: `RpcTimeoutError`, `RpcServerError`, `NotAuthorizedError`, `ServerBusyError`

### 5.3 stream() API 명세

스트리밍 RPC 호출.

| 형식 | 설명 |
|------|------|
| `stream(action[, params])` | 생성자 바인딩 필요 |
| `stream(thing_type, service, action, vin[, params])` | 명시 라우팅 |

`MaasClient.stream()`은 동기 `for` 이터레이터, `MaasClientAsync.stream()`은 `async for` 이터레이터를 반환한다.

각 아이템: `StreamEvent` (`.payload`, `.is_eof`, `.correlation_id`)

`is_eof=True`인 항목은 이터레이터에 포함되지 않는다 (SDK 내부에서 소비 후 이터레이터 종료).

### 5.4 exclusive_session() API 명세

독점 세션 컨텍스트 매니저 (패턴 E).

| 형식 | 설명 |
|------|------|
| `exclusive_session()` | 생성자 바인딩 필요 |
| `exclusive_session(thing_type, service, vin)` | 명시 라우팅 |

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `acquire_action` | `"session_start"` | Lock 획득 RPC action 이름 |
| `release_action` | `"session_stop"` | Lock 해제 RPC action 이름 |
| `timeout` | `15.0` | acquire/release 및 세션 내 call() 기본 timeout |

컨텍스트 진입 시 `acquire_action` RPC 호출, 종료 시 `release_action` RPC 호출. 세션 내에서 `session.call(action, params)` 사용.

### 5.5 동기/비동기 인터페이스

| 클래스 | 환경 | 내부 구조 |
|--------|------|-----------|
| `MaasClient` | 비async Python 스크립트, Flask 등 | asyncio 루프를 전용 데몬 스레드에서 운영. `run_coroutine_threadsafe()`로 결과 블로킹 대기 |
| `MaasClientAsync` | asyncio 환경 | asyncio 네이티브. `async def connect/disconnect/call/stream` |

두 클래스의 공개 API 인자 규칙은 동일하다.

---

## 6. 공통 비기능 요구사항

### 6.1 성능

- 단일 RPC 왕복 레이턴시 목표: 로컬 브로커 기준 p99 < 50ms (네트워크 제외)
- 동시 처리: 서버 측 asyncio 이벤트 루프로 100 RPS 비블로킹 처리

### 6.2 신뢰성 (연결 끊김, 재연결)

- 클라이언트 연결 끊김: `_pending`의 모든 대기 Future를 즉시 `ConnectionError`로 reject. 재연결 후 새 호출로 재시도
- 서버 독점 세션: 클라이언트 단절 시 `OfflineMonitor`(LWT) → `ExclusiveSessionManager.force_release_by_client()`로 세션 강제 해제
- 타임아웃 후 Pending Map 항목 반드시 제거 (메모리 누수 방지)

### 6.3 보안 (토큰 갱신, Clean Start)

- `token_provider` 콜백은 `connect()` 시마다 호출되어 최신 토큰을 주입한다
- `Clean Start = True`를 항상 사용하여 stale 응답이 재연결 후 도달하는 것을 방지한다
- `NotAuthorizedError` (reason_code=0x87)는 일반 타임아웃과 구분 가능한 예외로 전달한다
