# SDK 상세설계사양서

> 문서 유형: SDK 상세설계사양서
> 독자: SDK 구현·유지보수 담당 개발자
> 기준: 현재 구현 코드 (`sdk/python/client/`, `sdk/python/server/`)

---

## 1. 개요

본 문서는 `maas-client-sdk`와 `maas-server-sdk`의 내부 아키텍처, 자료구조, 스레딩 모델, 어댑터 패턴을 현재 구현 코드를 기반으로 기술한다.

공개 API 명세는 [SDK 요구사양서.md](SDK%20%EC%9A%94%EA%B5%AC%EC%82%AC%EC%96%91%EC%84%9C.md)를 참고한다.

---

## 2. maas-client-sdk 상세 설계

### 2.1 레이어 구조

```
MaasClient (동기 Facade, client.py)
  │  asyncio.run_coroutine_threadsafe()
  └─ 전용 asyncio 루프 (daemon 스레드)
       └─ MaasClientAsync (client_async.py)
            ├─ Mqtt5Connection (connection.py)  ← paho 콜백 → asyncio 브릿지
            ├─ RpcManager (_rpc.py)             ← Pending Map / Stream Map
            └─ PubSubManager (_pubsub.py)       ← 임의 pub/sub
```

`MaasClientAsync`는 의존성 세 개(`Mqtt5Connection`, `RpcManager`, `PubSubManager`)를 생성자에서 직접 구성한다. 수신 메시지는 `Mqtt5Connection.set_message_callback(self._dispatch_message)`으로 등록된 콜백을 통해 `RpcManager.handle_incoming()` → (미처리 시) `PubSubManager.handle_incoming()` 순으로 전달된다.

### 2.2 동기 Facade 패턴 (MaasClient)

`MaasClient`는 `MaasClientAsync`를 래핑하여 블로킹 인터페이스를 제공한다.

**생성자에서 백그라운드 루프 시작:**

```python
self._loop = asyncio.new_event_loop()
self._thread = threading.Thread(
    target=self._loop.run_forever,
    name="maas-client-loop",
    daemon=True,
)
self._thread.start()
```

**코루틴 실행 패턴:**

```python
def _run(self, coro, timeout=None):
    future = asyncio.run_coroutine_threadsafe(coro, self._loop)
    return future.result(timeout=timeout)
```

`call()`은 `_run(self._async.call(...), timeout=timeout + 5.0)`으로 호출한다. `+5.0`은 asyncio 레이어의 `RpcTimeoutError`가 먼저 발생할 시간을 확보하기 위한 여유값이다.

**스트리밍 동기화:**

`stream()`은 `asyncio.Queue`를 사용할 수 없으므로 `queue.Queue`(동기)를 브릿지로 사용한다.

```python
sync_queue = queue.Queue()

async def _collect():
    async for event in self._async.stream(...):
        sync_queue.put(event)
    sync_queue.put(None)  # 종료 sentinel

asyncio.run_coroutine_threadsafe(_collect(), self._loop)

while True:
    item = sync_queue.get(timeout=chunk_timeout)
    if item is None: break
    if not item.is_eof: yield item
```

**종료 순서:**

```python
def disconnect(self):
    try:
        self._run(self._async.disconnect(), timeout=10.0)
    finally:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5.0)
```

### 2.3 RpcManager 자료구조

`RpcManager` (`_rpc.py`)는 두 개의 핵심 자료구조를 관리한다.

#### Pending Map: `dict[bytes, asyncio.Future]`

```python
self._pending: dict[bytes, asyncio.Future] = {}
```

- 키: `correlation_id` (UUID v4 bytes, 16 bytes)
- 값: `asyncio.Future` — resolve 시 `RpcResponse` 설정, 오류 시 예외 설정
- 동시성 보호: `asyncio.Lock` (`self._lock`)

`call()` 진입 시 등록, 응답 수신 또는 timeout 시 `pop()` 제거.

#### Stream Map: `dict[bytes, asyncio.Queue]`

```python
self._streams: dict[bytes, asyncio.Queue] = {}
```

- 키: `correlation_id`
- 값: `asyncio.Queue` — `StreamEvent` 또는 `Exception` 인스턴스를 enqueue
- 동시성 보호: `asyncio.Lock` (`self._lock`)

`stream()` 진입 시 등록, is_eof=True 수신 또는 예외 발생 후 `finally` 블록에서 `pop()` 제거.

### 2.4 요청-응답 상관관계 처리 흐름

```
call() 호출
  │
  ├─ new_correlation_id() → UUID v4 bytes
  ├─ _pending[corr_id] = asyncio.Future
  ├─ build_publish_properties(response_topic, correlation_data, message_expiry)
  ├─ conn.publish(request_topic, raw, qos, props)  ← asyncio.to_thread로 paho 호출
  └─ asyncio.wait_for(future, timeout)
       │
       ├─ 성공: future.set_result(RpcResponse)  ← handle_incoming에서
       │        _pending.pop(corr_id)
       │        return RpcResponse
       │
       └─ 타임아웃: asyncio.TimeoutError
                 → _pending.pop(corr_id)
                 → raise RpcTimeoutError
```

`handle_incoming()`은 paho 콜백 스레드에서 `call_soon_threadsafe()`를 통해 asyncio 루프로 전달된 후 동기적으로 실행된다.

```python
def handle_incoming(self, msg: IncomingMessage) -> bool:
    corr = msg.correlation_data
    suffix = msg.topic.rsplit("/", 1)[-1]
    if suffix == "response":
        return self._handle_response(corr, msg)
    return False
```

`_handle_response()`에서:
- `corr in self._streams` → 스트림 처리 분기
- `self._pending.pop(corr, None)` → Future resolve 또는 set_exception

### 2.5 스트리밍 처리 흐름 (is_EOF 분기)

```
stream() 호출
  │
  ├─ corr_id 생성
  ├─ _streams[corr_id] = asyncio.Queue()
  ├─ conn.publish(request_topic, ...)
  └─ while True:
       item = await q.get()
       │
       ├─ isinstance(item, Exception) → raise
       ├─ item.is_eof == False → yield item
       └─ item.is_eof == True  → yield item → break
                                 (finally: _streams.pop(corr_id))
```

`_handle_response()`의 스트림 분기:

```python
if corr in self._streams:
    q = self._streams.get(corr)
    if rc != 0:
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(q.put(_make_error(rc, detail))))
    else:
        event = StreamEvent(payload=payload, is_eof=is_eof, correlation_id=corr)
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(q.put(event)))
    return True
```

`loop.call_soon_threadsafe` + `asyncio.ensure_future(q.put(...))`로 paho 콜백 스레드에서 asyncio Queue에 안전하게 enqueue한다.

### 2.6 연결 관리 (Mqtt5Connection: paho 콜백 → asyncio 브릿지)

`Mqtt5Connection` (`connection.py`)은 paho-mqtt 2.x의 스레드 기반 콜백을 asyncio 루프로 안전하게 브릿지한다.

**연결 흐름:**

```python
async def connect(self):
    self._loop = asyncio.get_running_loop()    # 루프 캡처
    self._mqtt = mqtt.Client(protocol=mqtt.MQTTv5, ...)
    self._mqtt.on_connect = self._on_connect
    self._mqtt.on_disconnect = self._on_disconnect
    self._mqtt.on_message = self._on_message

    await asyncio.to_thread(
        self._mqtt.connect, endpoint, port, keepalive=30, clean_start=True
    )
    self._mqtt.loop_start()   # paho 내부 네트워크 스레드 시작

    connected = await asyncio.to_thread(self._connect_event.wait, 30.0)
```

paho `loop_start()`는 paho 전용 백그라운드 스레드를 시작한다. 이 스레드에서 `_on_connect`, `_on_disconnect`, `_on_message` 콜백이 호출된다.

**메시지 수신 브릿지:**

```python
def _on_message(self, client, userdata, message):
    incoming = IncomingMessage(message)   # paho msg → DTO 변환
    self._loop.call_soon_threadsafe(self._message_callback, incoming)
```

`call_soon_threadsafe()`는 paho 스레드에서 asyncio 루프 스레드로 안전하게 콜백을 예약한다. `_message_callback`은 `MaasClientAsync._dispatch_message`이며, asyncio 루프 컨텍스트에서 동기적으로 실행된다.

**PUBLISH는 `asyncio.to_thread` 경유:**

```python
async def publish(self, topic, payload, qos=0, properties=None):
    await asyncio.to_thread(self._mqtt.publish, topic, payload, qos, False, properties)
```

paho `publish()`는 동기 블로킹 API이므로 `asyncio.to_thread()`로 스레드 풀에서 실행한다.

---

## 3. maas-server-sdk 상세 설계

### 3.1 레이어 구조

```
MaasServer (server.py)
  ├─ MqttClientAdapter (Protocol, _adapter.py)
  │    ├─ PahoMqttAdapter (_adapters.py)    ← mode="mqtt"
  │    └─ GreengrassIpcAdapter (_adapters.py) ← mode="greengrass"
  ├─ Dispatcher (_dispatcher.py)            ← 라우팅 + 응답 발행
  ├─ ExclusiveSessionManager (session.py)   ← 서비스 단위 단일 세션 (exclusive_service=True 시)
  └─ OfflineMonitor (presence.py)           ← LWT(offline) 감지 → force_release_by_client
```

`MaasServer.__init__()`에서 `mode` 파라미터에 따라 어댑터를 선택하고, `Dispatcher`·`ExclusiveSessionManager`·`OfflineMonitor`를 구성한다.

```python
if mode == "mqtt":
    adapter = PahoMqttAdapter(endpoint, client_id, vin, port, use_wss)
elif mode == "greengrass":
    adapter = GreengrassIpcAdapter(vin=vin)

self._adapter = adapter
self._dispatcher = Dispatcher(conn=self._adapter, ...)
self._presence.on_disconnect(self._session.force_release)
self._adapter.set_message_callback(self._dispatch)
```

### 3.2 MqttClientAdapter 어댑터 패턴

#### Protocol 정의 (`_adapter.py`)

```python
@runtime_checkable
class MqttClientAdapter(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def publish(self, topic: str, payload: bytes, qos: int,
                props: Optional[MqttProperties]) -> None: ...
    def subscribe(self, topic: str, qos: int) -> None: ...
    def set_message_callback(self, callback: MessageCallback) -> None: ...
    def get_vin(self) -> str: ...
```

모든 메서드는 동기(블로킹). 비동기 컨텍스트에서는 `asyncio.to_thread()`로 감싼다.

```python
@dataclass
class MqttProperties:
    correlation_data: Optional[bytes] = None
    response_topic: Optional[str] = None
    user_properties: list[tuple[str, str]] = field(default_factory=list)
    message_expiry_interval: Optional[int] = None
```

`MqttProperties`는 paho `Properties`와 Greengrass IPC 헤더 모두에서 독립된 중립 표현이다. 각 어댑터의 `publish()`에서 전송 계층에 맞게 변환한다.

#### PahoMqttAdapter (`_adapters.py`, mode="mqtt")

paho-mqtt 2.x `mqtt.Client`를 직접 래핑한다.

- `connect()`: 동기 블로킹. `threading.Event`로 연결 완료 대기
- `publish()`: `MqttProperties` → `paho_props.Properties` 변환 후 `mqtt.publish()`
- `subscribe()`: `mqtt.subscribe()` 직접 호출
- `_on_message()`: paho 콜백 스레드에서 `IncomingMessage`로 변환 후 `self._message_callback(incoming)` 직접 호출

`PahoMqttAdapter`는 서버 측이므로 클라이언트와 달리 `call_soon_threadsafe`를 사용하지 않는다. `_on_message()`에서 바로 콜백을 호출하고, `MaasServer._dispatch()`에서 `asyncio.run_coroutine_threadsafe(self._dispatcher.handle(msg), self._loop)`로 asyncio 루프에 넘긴다.

#### GreengrassIpcAdapter (`_adapters.py`, mode="greengrass")

AWS Greengrass Core IPC를 래핑한다. `awsiotsdk`는 `connect()` 메서드 본체 내부에서만 지연 import한다.

```python
def connect(self):
    try:
        import awsiot.greengrasscoreipc as gg_ipc
        import awsiot.greengrasscoreipc.clientV2 as gg_v2
    except ImportError as exc:
        raise ImportError("pip install maas-server-sdk[aws]") from exc
    self._ipc_client = gg_v2.GreengrassCoreIPCClientV2()
    if self._vin is None:
        self._vin = self._get_thing_name()   # AWS_IOT_THING_NAME 환경변수
```

`publish()`: `MqttProperties` → `PublishToIoTCoreRequest` 변환

`subscribe()`: `subscribe_to_iot_core()` 호출 + `_IpcMessageStreamHandler.on_stream_event()` 콜백 등록

IPC 수신 메시지: `_ipc_event_to_incoming(event)` → `IncomingMessage.from_raw(...)` → `self._message_callback(incoming)` 호출

### 3.3 asyncio 이벤트 루프 구조 (_run_async, stop_event)

`MaasServer.run()`은 `asyncio.run(self._run_async())`로 블로킹 실행한다.

```python
async def _run_async(self):
    self._loop = asyncio.get_running_loop()
    self._stop_event = asyncio.Event()

    await asyncio.to_thread(self._adapter.connect)      # 블로킹 connect를 스레드에서

    if self._vin is None:                               # Greengrass 모드: VIN 자동 취득
        self._vin = self._adapter.get_vin()
        self._dispatcher.update_vin(self._vin)

    await asyncio.to_thread(self._adapter.subscribe, sub_topic, 1)
    # ... lifecycle, pubsub 구독 ...

    await self._stop_event.wait()                        # 블로킹 대기 (KeyboardInterrupt 포함)
    # ...
    await asyncio.to_thread(self._adapter.disconnect)
```

`stop()`은 `self._loop.call_soon_threadsafe(self._stop_event.set)`으로 루프에 안전하게 신호를 보낸다.

수신 메시지는 `_dispatch()` 메서드에서 처리된다.

```python
def _dispatch(self, msg: IncomingMessage):
    if self._presence.matches_lifecycle_topic(msg.topic):
        self._presence.handle_message(msg.topic, msg.payload)
        return
    for pattern, callbacks in self._dispatcher._pubsub_handlers.items():
        if _topic_matches(pattern, msg.topic):
            for cb in callbacks: cb(msg.topic, msg.payload)
            return
    asyncio.run_coroutine_threadsafe(
        self._dispatcher.handle(msg), self._loop
    )
```

어댑터 콜백 스레드에서 `run_coroutine_threadsafe()`로 asyncio 루프에 `Dispatcher.handle(msg)`를 제출한다.

### 3.4 Dispatcher 라우팅 알고리즘

`Dispatcher.handle(msg: IncomingMessage)` 처리 순서:

1. `topic_utils.parse_request(msg.topic)` → ThingType / Service / VIN / ClientId 추출
2. `parsed.thing_type == self._thing_type and ...` — 이 서버 담당 토픽인지 검증
3. JSON 페이로드 파싱. 실패 시 `reason_code=0x99` 응답
4. **route_key 분기:**
   - `route_key is None` → `_handlers[HANDLER_DEFAULT_KEY]` 조회. 없으면 오류
   - `route_key` 문자열 → `payload.pop(route_key)` 추출 → `_handlers[route_label]` 조회
   - 핸들러 없으면 `reason_code=0x90` 응답
5. `RpcContext` 구성 (payload에서 route_key 필드 제거 완료)
6. 패턴 E 접근 제어: `exclusive_mgr.owner()`가 다른 client_id면 `0x8A` 자동 거부
7. `_invoke_streaming()` 또는 `_invoke_single()` 실행 (실행 후 session_id 변화를 응답 User Property로 신호)

**핸들러 실행 (`_invoke_single`):**

```python
if inspect.iscoroutinefunction(func):
    result = await func(ctx)
else:
    result = await asyncio.to_thread(func, ctx)
```

동기 핸들러는 `asyncio.to_thread()`로 스레드 풀에서 실행하여 이벤트 루프를 블로킹하지 않는다.

**응답 발행:**

```python
await asyncio.to_thread(self._conn.publish, ctx.response_topic, raw, 1, props)
```

어댑터의 동기 `publish()`를 `asyncio.to_thread()`로 감싸 non-blocking으로 호출한다.

### 3.5 ExclusiveSessionManager 메커니즘

`ExclusiveSessionManager` (`session.py`)는 서비스(=VIN) 단위 단일 독점 세션을 `threading.Lock`으로 관리한다. `MaasServer(exclusive_service=True)` 일 때만 생성된다.

**내부 상태:**

```python
self._current: Optional[tuple[str, str]] = None  # (client_id, session_id) 또는 None
self._mutex = threading.Lock()
```

**`acquire(client_id) -> Optional[str]`:**
- 미점유 상태면 새 `session_id`(uuid)를 발번해 점유하고 반환
- 동일 client_id 재획득이면 기존 session_id 반환
- 다른 client_id가 점유 중이면 `None`

**`release(client_id) -> bool`:** 점유자만 해제 가능.

**`force_release_by_client(client_id) -> Optional[str]`:**
- 해당 client_id가 점유 중이면 세션을 해제하고 session_id 반환
- `OfflineMonitor.on_offline` 콜백(LWT 단절)으로 연결됨

**Thread-safety:** 모든 읽기·쓰기는 `self._mutex`(`threading.Lock`) 내부에서 수행한다. asyncio 이벤트 루프와 paho 콜백 스레드 양쪽에서 호출되므로 threading.Lock이 필요하다.

디스패처는 매 요청마다 `owner()`가 다른 client_id면 핸들러 실행 전에 `0x8A`로 거부하고, 핸들러 실행 전후 `get_session_id()` 변화를 응답 `session_id` User Property로 신호화한다.

### 3.6 OfflineMonitor(LWT) → force_release 흐름

```
클라이언트 비정상 단절
  → 브로커가 LWT(will)를 .../offline 토픽으로 PUBLISH
  → PahoMqttAdapter._on_message() / GreengrassIpcAdapter.on_stream_event()
  → MaasServer._dispatch()
  → offline_monitor.matches(topic) → True
  → offline_monitor.handle_message(topic, payload)
     → 토픽(우선) 또는 payload에서 client_id 추출
     → _on_offline_callbacks 순서대로 호출
        → MaasServer._on_client_offline(client_id)
           → ExclusiveSessionManager.force_release_by_client() (세션 강제 해제)
           → SubscriptionRegistry.cancel_by_client() (패턴 C 구독 취소)
```

서버는 패턴 C/E 핸들러가 있으면 `_run_async()` 에서 `WMT/.../+/offline` 와일드카드를 자동 구독한다.

---

## 4. 공통 설계 결정 사항

### 4.1 QoS 1 Message Expiry = timeout 동기화 근거

구현: `_rpc.py::_publish_message_expiry_for_call()`

```python
if qos == 1:
    return max(1, ceil(timeout))
return expiry
```

**근거:** QoS 1에서 브로커는 클라이언트가 응답을 포기한 후에도 요청 메시지를 무기한 보관하여 나중에 전달할 수 있다. 이 경우 클라이언트는 이미 타임아웃 처리가 완료되었지만 서비스는 뒤늦게 실행된다. 특히 패턴 D(시한성 제어)에서 이는 위험하다.

`Message Expiry Interval = ceil(timeout)`으로 설정하면 클라이언트가 응답을 포기하는 시각과 브로커가 요청을 폐기하는 시각이 일치한다.

QoS 0은 브로커가 즉시 전달 또는 즉시 폐기(비큐잉)하는 것이 일반적이므로 `expiry`를 그대로 전달하며, 시한성 제어는 QoS 1을 권장한다.

### 4.2 Clean Start = True 정책 근거

클라이언트(`Mqtt5Connection.connect()`)와 서버(`PahoMqttAdapter.connect()`) 모두 `clean_start=True`로 연결한다.

**근거:** `Clean Start = False`로 연결하면 이전 세션에서 쌓인 QoS 1 메시지가 재연결 후 도착한다. 이 "stale 응답"은 `_pending` 맵에 대응하는 Future가 없어 무시되지만, 순간적으로 `handle_incoming`이 불필요한 작업을 수행한다. 더 중요하게는 이전 `correlation_id`가 우연히 현재 요청의 `correlation_id`와 충돌하면 잘못된 Future를 resolve할 수 있다. `Clean Start = True`로 이 위험을 원천 차단한다.

### 4.3 awsiotsdk 지연 import (CCU 환경 ImportError 방지)

`GreengrassIpcAdapter`는 `_adapters.py` 최상위 스코프에서 `awsiotsdk`를 import하지 않는다. import는 반드시 `connect()` 메서드 본체 내부에서만 수행한다.

**근거:** CCU 환경에는 `awsiotsdk`가 설치되어 있지 않다. `_adapters.py`를 모듈로 import하는 시점에 `awsiotsdk`가 필요하다면, `mode="mqtt"` 사용자도 `from maas_server import MaasServer`만으로 `ImportError`를 겪게 된다.

`GreengrassIpcAdapter` 클래스 정의 자체는 최상위에 있어도 무방하다. **import 문이 클래스 본체 밖(모듈 스코프)에 없으면** 모듈 로딩 시 `awsiotsdk`를 참조하지 않는다.

`server.py`에서도 어댑터를 지연 import로 선택한다.

```python
if mode == "greengrass":
    adapter = GreengrassIpcAdapter(vin=vin)   # 이 시점에 awsiotsdk import
elif mode == "mqtt":
    adapter = PahoMqttAdapter(...)            # awsiotsdk 불필요
```

선택적 설치: `pip install maas-server-sdk[aws]` (`awsiotsdk>=1.20`).
