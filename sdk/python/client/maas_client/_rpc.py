"""
RPC 레이어 (내부 모듈).

WMT/WMO 토픽 패턴 기반의 요청-응답 및 스트리밍 RPC 로직.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Optional

from .connection import (
    IncomingMessage,
    Mqtt5Connection,
    UP_CANCEL_REASON,
    UP_ERROR_DETAIL,
    UP_IS_EOF,
    UP_QOS,
    UP_REASON_CODE,
    UP_SENT_AT,
    UP_SESSION_ID,
    UP_SUBSCRIPTION_ID,
    UP_TIMEOUT,
    UP_UNSUBSCRIBE_ACTION,
    build_publish_properties,
    decode_payload,
    encode_payload,
    new_correlation_id,
)
from .exceptions import (
    RpcExpiredError,
    RpcServerError,
    RpcTimeoutError,
    NotAuthorizedError,
    ServerBusyError,
    SubscriptionCancelledError,
)
from .models import Response, RpcResponse, Session, StreamEvent, Subscription
from . import topics

logger = logging.getLogger(__name__)

_REASON_SUCCESS = 0
_RC_NOT_AUTHORIZED = 0x87
_RC_SERVER_BUSY = 0x8A


def _raise_for_reason_code(rc: int, detail: str, service: str, action: str) -> None:
    """reason_code가 오류면 적절한 예외를 발생시킨다."""
    if rc == 0:
        return
    if rc == _RC_NOT_AUTHORIZED:
        raise NotAuthorizedError(detail)
    if rc == _RC_SERVER_BUSY:
        raise ServerBusyError(detail)
    raise RpcServerError(rc, detail)


def _ceil_positive_seconds(seconds: float) -> int:
    """
    양의 타임아웃(초)을 정수 초로 올림한다.

    ``timeout``이 정수 초와 같으면 그대로 두고, 소수 초면 한 칸 올린다.
    """
    t = float(seconds)
    i = int(t)
    return i if t <= i else i + 1


def _publish_message_expiry_for_call(
    qos: int,
    timeout: float,
    expiry: Optional[int],
) -> Optional[int]:
    """
    단일 RPC PUBLISH에 넣을 Message Expiry Interval(초)을 결정한다.

    QoS 1에서는 브로커가 오래된 요청을 무기한 전달하지 않도록, 클라이언트의
    ``timeout``(응답 대기 상한)과 동일한 값을 Expiry로 둔다(초 단위 올림, 최소 1).
    QoS 0에서는 ``expiry`` 인자만 선택 반영한다. QoS 0은 보통 비큐잉이라
    Expiry의 실효는 배포·브로커에 따라 제한적일 수 있다.

    Args:
        qos: MQTT QoS (0 또는 1).
        timeout: ``call``의 응답 대기 타임아웃(초).
        expiry: 호출자가 지정한 Expiry. QoS 0에서만 PUBLISH에 넣는다.

    Returns:
        ``build_publish_properties``에 넘길 정수 초, 또는 생략 시 ``None``.
    """
    if qos == 1:
        return max(1, _ceil_positive_seconds(timeout))
    return expiry


def _build_request_payload(action: str, params: Any) -> bytes:
    """
    action과 RPC params를 MQTT PUBLISH 본문(JSON bytes)으로 직렬화.

    JSON 객체에서는 ``action`` 키를 항상 선행(첫 필드)으로 둔다.
    params에 ``action`` 키가 있어도 인자 ``action``이 최종 값으로 쓰인다.
    """
    if isinstance(params, dict):
        rest = {k: v for k, v in params.items() if k != "action"}
        data = {"action": action, **rest}
    elif params is None:
        data = {"action": action}
    else:
        data = {
            "action": action,
            "data": params
            if not isinstance(params, bytes)
            else params.decode("utf-8", errors="replace"),
        }
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


class RpcManager:
    """
    RPC 요청-응답 및 스트리밍을 관리한다.

    pending_map: correlation_id → asyncio.Future (단일 응답 대기)
    stream_map: correlation_id → asyncio.Queue (스트림 이벤트 대기)
    """

    def __init__(self, conn: Mqtt5Connection, client_id: str) -> None:
        self._conn = conn
        self._client_id = client_id
        self._pending: dict[bytes, asyncio.Future] = {}
        self._streams: dict[bytes, asyncio.Queue] = {}
        # 통합 call() 진행 중 항목: corr → (queue, routing dict). 첫 응답 분기에 사용.
        self._unified: dict[bytes, dict] = {}
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._hb_mgr: Optional[Any] = None  # HeartbeatManager, 외부에서 주입

    async def setup_subscriptions(self) -> None:
        """
        연결 후 호출. 이 클라이언트의 응답 토픽을 와일드카드로 구독.
        """
        resp_topic = topics.build_response_wildcard(self._client_id)
        await self._conn.subscribe(resp_topic, qos=1)
        logger.debug("RPC 응답 구독 완료: %s", resp_topic)

    def handle_incoming(self, msg: IncomingMessage) -> bool:
        """
        수신 메시지를 pending_map 또는 stream_map으로 라우팅.

        Returns:
            처리된 경우 True.
        """
        suffix = msg.topic.rsplit("/", 1)[-1] if "/" in msg.topic else ""

        if suffix == "heartbeat":
            if self._hb_mgr:
                return self._hb_mgr.handle_heartbeat(msg)
            return False

        corr = msg.correlation_data
        if not corr:
            return False

        if suffix == "response":
            return self._handle_response(corr, msg)
        return False

    def _handle_response(self, corr: bytes, msg: IncomingMessage) -> bool:
        rc = int(msg.user_props.get(UP_REASON_CODE, "0") or "0")
        detail = msg.user_props.get(UP_ERROR_DETAIL, "")
        is_eof = msg.user_props.get(UP_IS_EOF, "").lower() == "true"
        payload = decode_payload(msg.payload)

        # 스트림 처리 (청크 또는 EOF) — open_subscription/stream 경로
        if corr in self._streams:
            q = self._streams.get(corr)
            if q is not None:
                if rc != 0:
                    q.put_nowait(_make_error(rc, detail, payload))
                elif is_eof:
                    cancel_reason = msg.user_props.get(UP_CANCEL_REASON)
                    if cancel_reason:
                        q.put_nowait(SubscriptionCancelledError(cancel_reason))
                    else:
                        q.put_nowait(StreamEvent(
                            payload=payload, is_eof=True, correlation_id=corr
                        ))
                else:
                    q.put_nowait(StreamEvent(
                        payload=payload, is_eof=False, correlation_id=corr
                    ))
            return True

        # 통합 call() 첫 응답 분기
        if corr in self._unified:
            return self._handle_unified_first(corr, msg, rc, detail, is_eof, payload)

        # 단일 응답 처리
        future = self._pending.pop(corr, None)
        if future is None or future.done():
            return False

        if rc != 0:
            future.set_exception(_make_error(rc, detail, payload))
        else:
            future.set_result(
                Response(payload=payload, reason_code=rc, correlation_id=corr)
            )
        return True

    def _handle_unified_first(
        self,
        corr: bytes,
        msg: IncomingMessage,
        rc: int,
        detail: str,
        is_eof: bool,
        payload: Any,
    ) -> bool:
        """통합 call()의 첫 응답을 받아 단일/스트림/세션을 판별한다."""
        info = self._unified.get(corr)
        if info is None:
            return False
        future: asyncio.Future = info["future"]
        if future.done():
            return False

        # 오류 응답 → 즉시 예외
        if rc != 0:
            self._unified.pop(corr, None)
            future.set_exception(_make_error(rc, detail, payload))
            return True

        up = msg.user_props
        session_id = up.get(UP_SESSION_ID)
        sub_id = up.get(UP_SUBSCRIPTION_ID)
        if sub_id is None and isinstance(payload, dict):
            sub_id = payload.get("subscriptionId")
        unsub_action = up.get(UP_UNSUBSCRIBE_ACTION) or "unsubscribe"

        # 스트림 여부: is_EOF=false 인 경우에만 구독으로 본다.
        is_stream = (UP_IS_EOF in up) and not is_eof

        if is_stream:
            # 스트림: corr 를 _streams 로 승격하고 첫 이벤트를 큐에 적재.
            q: asyncio.Queue = info["queue"]
            self._streams[corr] = q
            self._unified.pop(corr, None)
            first_event = StreamEvent(
                payload=payload, is_eof=False, correlation_id=corr
            )
            resp = Response(
                payload=payload,
                reason_code=0,
                correlation_id=corr,
                is_subscription=True,
                subscription_id=sub_id,
                _rpc=self,
                _thing_type=info["thing_type"],
                _service=info["service"],
                _vin=info["vin"],
                _corr_id=corr,
                _queue=q,
                _unsubscribe_action=unsub_action,
                _first_event=first_event,
                _hb_topic=info.get("hb_topic"),
            )
            # 스트림 Heartbeat 감시 등록 (best-effort)
            hb_topic = info.get("hb_topic")
            if hb_topic and self._hb_mgr is not None:
                loop = self._loop or asyncio.get_event_loop()
                loop.create_task(
                    self._hb_mgr.watch_stream(hb_topic, corr, q)
                )
            future.set_result(resp)
            return True

        # 단일 응답 (세션 신호 포함 가능)
        self._unified.pop(corr, None)
        resp = Response(
            payload=payload,
            reason_code=0,
            correlation_id=corr,
            is_subscription=False,
            subscription_id=sub_id,
            _rpc=self,
            _thing_type=info["thing_type"],
            _service=info["service"],
            _vin=info["vin"],
            _corr_id=corr,
        )
        if session_id:  # 비어있지 않은 uuid → 활성 세션
            sess = Session(
                session_id=session_id,
                _rpc=self,
                _thing_type=info["thing_type"],
                _service=info["service"],
                _vin=info["vin"],
                _hb_topic=info.get("hb_topic"),
            )
            resp.session = sess
            self._arm_session_watch(sess)
        future.set_result(resp)
        return True

    def _arm_session_watch(self, sess: Session) -> None:
        """세션 Heartbeat watchdog 콜백 등록 (best-effort, fire-and-forget)."""
        if self._hb_mgr is None or not sess._hb_topic:
            return

        async def _register() -> None:
            try:
                wid = await self._hb_mgr.add_callback_watcher(
                    sess._hb_topic, sess._notify_lost
                )
                sess._watcher_id = wid
            except Exception:
                pass

        loop = self._loop or asyncio.get_event_loop()
        loop.create_task(_register())

    async def _unregister_stream(
        self, corr_id: bytes, hb_topic: Optional[str]
    ) -> None:
        """스트림 종료 시 _streams 및 HB 감시 해제 (Response 가 호출)."""
        async with self._lock:
            self._streams.pop(corr_id, None)
        if hb_topic and self._hb_mgr:
            try:
                await self._hb_mgr.unwatch_stream(hb_topic, corr_id)
            except Exception:
                pass

    async def call(
        self,
        thing_type: str,
        service: str,
        action: str,
        vin: str,
        params: Any = None,
        *,
        qos: int = 1,
        timeout: float = 10.0,
        expiry: Optional[int] = None,
        sent_at: bool = False,
    ) -> Response:
        """
        단일/스트림/세션 통합 RPC 호출. 첫 응답으로 종류를 판별해 ``Response`` 를 반환한다.

        Args:
            thing_type: 토픽의 {ThingType}.
            service: 토픽의 {Service}.
            action: 요청 JSON에 삽입할 action.
            vin: 토픽의 {VIN}.
            params: RPC 인자 (dict 권장). action 키는 SDK가 덮어쓴다.
            qos: MQTT QoS (0 또는 1). 요청 User Property ``qos`` 에도 실린다.
            timeout: 응답 대기 타임아웃(초). QoS 1이면 Message Expiry도 이 값과
                맞춘다(초 단위 올림, 최소 1초). 요청 User Property ``timeout`` 에도 실린다.
            expiry: Message Expiry Interval(초). **QoS 0일 때만** PUBLISH에 넣는다.
            sent_at: True이면(패턴 D) 요청 User Property ``sent_at`` (unix ms)을 추가하고
                미수신 시 ``RpcExpiredError`` 를 발생시킨다.

        Raises:
            RpcTimeoutError: 타임아웃 초과 (패턴 D면 ``RpcExpiredError``).
            RpcServerError: 서버 오류 응답.
        """
        loop = asyncio.get_running_loop()
        self._loop = loop
        corr_id = new_correlation_id()
        request_topic = topics.build_request(thing_type, service, vin, self._client_id)
        response_topic = topics.build_response(thing_type, service, vin, self._client_id)

        hb_topic: Optional[str] = None
        if self._hb_mgr is not None:
            hb_topic = topics.build_server_heartbeat(thing_type, service, vin)

        future: asyncio.Future = loop.create_future()
        queue: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._unified[corr_id] = {
                "future": future,
                "queue": queue,
                "thing_type": thing_type,
                "service": service,
                "vin": vin,
                "hb_topic": hb_topic,
            }

        eff_expiry = _publish_message_expiry_for_call(qos, timeout, expiry)
        user_props: list[tuple[str, str]] = [
            (UP_QOS, str(qos)),
            (UP_TIMEOUT, str(timeout)),
        ]
        if sent_at:
            user_props.append((UP_SENT_AT, str(int(time.time() * 1000))))
        props = build_publish_properties(
            response_topic=response_topic,
            correlation_data=corr_id,
            message_expiry=eff_expiry,
            user_props=user_props,
        )
        raw = _build_request_payload(action, params)

        try:
            await self._conn.publish(request_topic, raw, qos=qos, properties=props)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            async with self._lock:
                self._unified.pop(corr_id, None)
                self._streams.pop(corr_id, None)
            if sent_at:
                raise RpcExpiredError(service, action, timeout)
            raise RpcTimeoutError(service, action, timeout)
        except Exception:
            async with self._lock:
                self._unified.pop(corr_id, None)
                self._streams.pop(corr_id, None)
            raise

    async def stream(
        self,
        thing_type: str,
        service: str,
        action: str,
        vin: str,
        params: Any = None,
        *,
        qos: int = 1,
    ) -> AsyncIterator[StreamEvent]:
        """
        스트리밍 RPC 호출. EOF 신호가 올 때까지 StreamEvent를 yield한다.

        Args:
            thing_type: 토픽의 {ThingType}.
            service: 토픽의 {Service}.
            action: 요청 JSON에 삽입할 action.
            vin: 토픽의 {VIN}.
            params: RPC 인자.
            qos: MQTT QoS.

        Yields:
            StreamEvent (is_eof=False인 청크들, 마지막은 is_eof=True).

        Raises:
            RpcServerError: 서버 오류.
            ServerOfflineError: 연결 끊김(HB 감시).
        """
        self._loop = asyncio.get_running_loop()
        corr_id = new_correlation_id()
        request_topic = topics.build_request(thing_type, service, vin, self._client_id)
        response_topic = topics.build_response(thing_type, service, vin, self._client_id)

        q: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._streams[corr_id] = q

        props = build_publish_properties(
            response_topic=response_topic,
            correlation_data=corr_id,
        )
        raw = _build_request_payload(action, params)

        try:
            await self._conn.publish(request_topic, raw, qos=qos, properties=props)
            while True:
                item = await q.get()
                if isinstance(item, Exception):
                    raise item
                yield item
                if item.is_eof:
                    break
        finally:
            async with self._lock:
                self._streams.pop(corr_id, None)

    async def _stream_from_queue(
        self,
        thing_type: str,
        service: str,
        action: str,
        vin: str,
        params: Any = None,
        *,
        qos: int = 1,
        corr_id: bytes,
        queue: asyncio.Queue,
        hb_topic: Optional[str] = None,
    ) -> AsyncIterator[StreamEvent]:
        """미리 등록된 queue를 사용하는 스트림 generator."""
        request_topic = topics.build_request(thing_type, service, vin, self._client_id)
        response_topic = topics.build_response(thing_type, service, vin, self._client_id)
        self._loop = asyncio.get_running_loop()

        props = build_publish_properties(
            response_topic=response_topic,
            correlation_data=corr_id,
        )
        raw = _build_request_payload(action, params)

        try:
            await self._conn.publish(request_topic, raw, qos=qos, properties=props)
            while True:
                item = await queue.get()
                if isinstance(item, Exception):
                    raise item
                yield item
                if item.is_eof:
                    break
        finally:
            async with self._lock:
                self._streams.pop(corr_id, None)
            if hb_topic and self._hb_mgr:
                await self._hb_mgr.unwatch_stream(hb_topic, corr_id)

    async def open_subscription(
        self,
        thing_type: str,
        service: str,
        action: str,
        vin: str,
        params: Any = None,
        *,
        qos: int = 1,
    ) -> Subscription:
        """
        패턴 C: 무한 구독. 첫 이벤트에서 subscriptionId를 추출 후 Subscription 반환.

        Args:
            thing_type: 토픽의 {ThingType}.
            service: 토픽의 {Service}.
            action: 요청 JSON에 삽입할 action.
            vin: 토픽의 {VIN}.
            params: RPC 인자.
            qos: MQTT QoS.

        Returns:
            Subscription (subscription_id, events async iterator, unsubscribe()).
        """
        corr_id = new_correlation_id()
        hb_topic: Optional[str] = None

        if self._hb_mgr is not None:
            from . import topics as _topics
            hb_topic = _topics.build_server_heartbeat(thing_type, service, vin)

        # queue를 먼저 생성 후 _streams에 등록
        q: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._streams[corr_id] = q

        # Heartbeat 감시 등록 (queue가 _streams에 등록된 직후)
        if hb_topic and self._hb_mgr:
            await self._hb_mgr.watch_stream(hb_topic, corr_id, q)

        # 미리 생성된 queue를 사용하는 generator 사용
        gen = self._stream_from_queue(
            thing_type, service, action, vin, params, qos=qos,
            corr_id=corr_id, queue=q, hb_topic=hb_topic,
        )

        try:
            first = await gen.__anext__()
        except StopAsyncIteration:
            return Subscription(
                subscription_id=None,
                events=_empty_aiter(),
                _rpc=self,
                _thing_type=thing_type,
                _service=service,
                _vin=vin,
            )
        if first.is_eof:
            return Subscription(
                subscription_id=None,
                events=_empty_aiter(),
                _rpc=self,
                _thing_type=thing_type,
                _service=service,
                _vin=vin,
            )
        sub_id = first.payload.get("subscriptionId") if isinstance(first.payload, dict) else None
        return Subscription(
            subscription_id=sub_id,
            events=gen,
            _rpc=self,
            _thing_type=thing_type,
            _service=service,
            _vin=vin,
        )


async def _empty_aiter():
    """빈 async iterator. 즉시 종료."""
    return
    yield


def _make_error(rc: int, detail: str, error_body: Any = None) -> RpcServerError:
    """reason_code에 맞는 예외 인스턴스 생성."""
    if rc == _RC_NOT_AUTHORIZED:
        err = NotAuthorizedError(detail, error_body=error_body)
    elif rc == _RC_SERVER_BUSY:
        err = ServerBusyError(detail, error_body=error_body)
    else:
        err = RpcServerError(rc, detail, error_body=error_body)
    return err
