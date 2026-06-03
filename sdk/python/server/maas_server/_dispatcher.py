"""
RPC 디스패처 (내부 모듈).

수신된 요청 토픽과 페이로드를 파싱하여
등록된 핸들러 함수로 라우팅하고, 응답을 자동 발행한다.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .connection import (
    IncomingMessage,
    UP_CANCEL_REASON,
    UP_IS_EOF,
    UP_QOS,
    UP_SENT_AT,
    UP_SESSION_ID,
    UP_SUBSCRIPTION_ID,
    UP_TIMEOUT,
    UP_UNSUBSCRIBE_ACTION,
)
from ._adapter import MqttClientAdapter, MqttProperties, encode_payload
from .context import RpcContext
from .exceptions import HandlerError
from .session import ExclusiveSessionManager
from .subscription import SubscriptionRegistry
from . import topics as topic_utils

logger = logging.getLogger(__name__)

# MQTT5 User Property reason_code 값
_RC_UNSPECIFIED = 0x80
_RC_PAYLOAD_INVALID = 0x99
_RC_SERVER_BUSY = 0x8A

# @server.default 전용 내부 라우트 키 (클라이언트 action 이름과 충돌하지 않도록 길게 둔다)
HANDLER_DEFAULT_KEY = "__maas_sdk_default__"

# 기본 unsubscribe action 이름 (패턴 C 클라이언트 주도 취소)
DEFAULT_UNSUBSCRIBE_ACTION = "unsubscribe"

HandlerFunc = Callable[[RpcContext], Any]


@dataclass
class HandlerEntry:
    """등록된 핸들러 메타데이터."""

    func: HandlerFunc
    subscription: bool = False
    stream_qos: int = 0


def _make_middleware_chain(mw, ctx: RpcContext, inner):
    """미들웨어를 call_next로 감싼 코루틴 팩토리."""
    async def chained():
        async def call_next_fn(_ctx=None):
            await inner()
        await mw(ctx, call_next_fn)
    return chained


class Dispatcher:
    """
    요청 토픽 + 페이로드 라우터.

    페이로드에서 ``route_key`` 로 지정한 필드(기본 ``action``)를 꺼내 핸들러를 조회한다.
    ``route_key=None`` 이면 페이로드를 나누지 않고 ``@server.default`` 핸들러만 호출한다.
    토픽의 ``{Service}`` 는 ``MaasServer.service_name`` 과 일치할 때만 처리한다.
    응답은 SDK가 자동 발행한다.
    """

    def __init__(
        self,
        conn: MqttClientAdapter,
        thing_type: str,
        service_name: str,
        vin: str,
        route_key: Optional[str] = "action",
        *,
        exclusive_mgr: Optional[ExclusiveSessionManager] = None,
        clock_tolerance_ms: int = 500,
    ) -> None:
        self._conn = conn
        self._exclusive_mgr = exclusive_mgr
        self._clock_tolerance_ms = clock_tolerance_ms
        self._thing_type = thing_type
        self._service_name = service_name
        self._vin = vin
        self._route_key = route_key
        self._handlers: dict[str, HandlerEntry] = {}
        self._pubsub_handlers: dict[str, list[Callable]] = {}
        self._middlewares: list = []
        self._registry = SubscriptionRegistry()
        self._unsubscribe_action = DEFAULT_UNSUBSCRIBE_ACTION

    @property
    def has_subscription_handler(self) -> bool:
        """등록된 핸들러 중 구독(스트리밍) 핸들러가 있는지."""
        return any(e.subscription for e in self._handlers.values())

    def add_middleware(self, func) -> None:
        """미들웨어 등록. 등록 역순으로 실행된다 (last-in, first-executed)."""
        self._middlewares.append(func)

    def _build_chain(self, ctx: RpcContext, core_coro) -> "asyncio.coroutine":
        """
        미들웨어 체인을 구성하여 실행할 코루틴을 반환.
        미들웨어는 등록 역순(last-in, first-executed)으로 실행된다.
        """
        executed = [False]

        async def execute():
            executed[0] = True
            await core_coro

        call_next = execute

        for mw in self._middlewares:
            call_next = _make_middleware_chain(mw, ctx, call_next)

        async def run():
            try:
                await call_next()
            finally:
                if not executed[0]:
                    core_coro.close()

        return run()

    def register(
        self,
        action: str,
        func: HandlerFunc,
        *,
        subscription: bool = False,
        stream_qos: int = 0,
    ) -> None:
        """
        핸들러 등록.

        ``action`` 은 페이로드의 ``route_key`` 필드 값(기본 필드명 ``action``)과 같아야 한다.
        ``route_key=None`` 인 서버에서는 사용할 수 없다 (``register_default`` 만 허용).
        ``subscription=True`` 면 generator/async generator 스트리밍 핸들러다.
        """
        if self._route_key is None:
            raise ValueError(
                "MaasServer(route_key=None)에서는 @server.action을 쓸 수 없습니다. "
                "@server.default 하나만 등록하세요."
            )
        if action == HANDLER_DEFAULT_KEY:
            raise ValueError(
                f"action 이름 {HANDLER_DEFAULT_KEY!r} 은 SDK 내부용이므로 사용할 수 없습니다."
            )
        if action in self._handlers:
            logger.warning("핸들러 중복 등록: action=%s", action)
        self._handlers[action] = HandlerEntry(
            func=func,
            subscription=subscription,
            stream_qos=stream_qos,
        )
        logger.debug("핸들러 등록: action=%s", action)

    def register_default(
        self,
        func: HandlerFunc,
        *,
        subscription: bool = False,
        stream_qos: int = 0,
    ) -> None:
        """
        기본 핸들러 등록.

        - ``route_key`` 가 문자열일 때: 해당 필드가 없거나 빈 문자열이면 이 핸들러가 호출된다.
        - ``route_key=None`` 일 때: 페이로드 전체를 넘기며 이 핸들러만 사용한다.
        """
        if HANDLER_DEFAULT_KEY in self._handlers:
            logger.warning("default 핸들러 중복 등록")
        self._handlers[HANDLER_DEFAULT_KEY] = HandlerEntry(
            func=func,
            subscription=subscription,
            stream_qos=stream_qos,
        )
        logger.debug("default 핸들러 등록")

    def update_vin(self, vin: str) -> None:
        """VIN 갱신 (Greengrass 어댑터에서 자동 취득 후 호출)."""
        self._vin = vin

    def register_pubsub(self, topic: str, func: Callable) -> None:
        """pub/sub 핸들러 등록."""
        if topic not in self._pubsub_handlers:
            self._pubsub_handlers[topic] = []
        self._pubsub_handlers[topic].append(func)

    async def handle(self, msg: IncomingMessage) -> None:
        """수신 메시지 처리."""
        parsed = topic_utils.parse_request(msg.topic)
        if not parsed:
            return

        if (
            parsed.thing_type != self._thing_type
            or parsed.service != self._service_name
            or parsed.vin != self._vin
        ):
            logger.debug(
                "이 서버와 일치하지 않는 토픽 무시: %s (기대: %s/%s/%s)",
                msg.topic,
                self._thing_type,
                self._service_name,
                self._vin,
            )
            return

        try:
            payload_dict = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            await self._reply_error(msg, _RC_PAYLOAD_INVALID, "페이로드 JSON 파싱 실패")
            return

        if not isinstance(payload_dict, dict):
            await self._reply_error(
                msg, _RC_PAYLOAD_INVALID, "JSON 페이로드는 객체(dict)여야 합니다"
            )
            return

        route_label: str
        remaining: dict[str, Any]
        entry: Optional[HandlerEntry]

        if self._route_key is None:
            remaining = dict(payload_dict)
            entry = self._handlers.get(HANDLER_DEFAULT_KEY)
            if not entry:
                await self._reply_error(
                    msg,
                    _RC_PAYLOAD_INVALID,
                    "route_key=None 인 서버에 @server.default 핸들러가 없습니다",
                )
                return
            route_label = ""
        else:
            remaining = dict(payload_dict)
            raw_route = remaining.pop(self._route_key, None)
            if raw_route is None or raw_route == "":
                entry = self._handlers.get(HANDLER_DEFAULT_KEY)
                if not entry:
                    await self._reply_error(
                        msg,
                        _RC_PAYLOAD_INVALID,
                        f"라우팅 필드 누락 또는 빈 값: {self._route_key!r}",
                    )
                    return
                route_label = ""
            else:
                route_label = (
                    raw_route if isinstance(raw_route, str) else str(raw_route)
                )
                entry = self._handlers.get(route_label)
                if not entry:
                    logger.warning("핸들러 없음: route=%s", route_label)
                    await self._reply_error(
                        msg, 0x90, f"미지원 라우트: {route_label}"
                    )
                    return

        user_props = dict(msg.user_props)
        ctx = RpcContext(
            thing_type=parsed.thing_type,
            service=parsed.service,
            action=route_label,
            vin=parsed.vin,
            client_id=parsed.client_id,
            payload=remaining,
            correlation_id=msg.correlation_data,
            response_topic=msg.response_topic,
            user_props=user_props,
            received_at=_monotonic(),
            request_qos=user_props.get(UP_QOS, "1") or "1",
        )
        ctx.sent_at_ms = _parse_int(user_props.get(UP_SENT_AT))
        ctx.timeout_s = _parse_float(user_props.get(UP_TIMEOUT))

        # 패턴 D: 시한 만료 검증 → 핸들러 미호출·응답 없음
        if ctx.is_expired(self._clock_tolerance_ms):
            logger.warning(
                "시한 만료 요청 폐기: action=%s, sent_at=%s, timeout=%s",
                ctx.action, ctx.sent_at_ms, ctx.timeout_s,
            )
            return

        # 패턴 E: 독점 서비스 접근 제어 (SDK 자동 0x8A 거부)
        if self._exclusive_mgr is not None:
            owner = self._exclusive_mgr.owner()
            if owner is not None and owner != ctx.client_id:
                await self._reply_error(
                    msg, _RC_SERVER_BUSY, f"독점 세션 점유 중: {owner}"
                )
                return

        # 패턴 E: session_id 신호 — 핸들러 실행 전후 스냅샷 비교
        sid_before: Optional[str] = None
        if self._exclusive_mgr is not None:
            sid_before = self._exclusive_mgr.get_session_id(ctx.client_id)

        if entry.subscription:
            core = self._invoke_streaming(ctx, entry.func, entry.stream_qos)
        else:
            core = self._invoke_single(ctx, entry.func, sid_before)

        try:
            if self._middlewares:
                await self._build_chain(ctx, core)
            else:
                await core
        except HandlerError as exc:
            await self._reply_error_ctx(ctx, exc.reason_code, str(exc), body=exc.body)
        except Exception:
            logger.exception("미들웨어 체인 예외: action=%s", ctx.action)
            await self._reply_error_ctx(ctx, _RC_UNSPECIFIED, "middleware error")

    async def _invoke_single(
        self,
        ctx: RpcContext,
        func: HandlerFunc,
        sid_before: Optional[str] = None,
    ) -> None:
        """단일 응답 핸들러 실행."""
        try:
            if inspect.iscoroutinefunction(func):
                result = await func(ctx)
            else:
                result = await asyncio.to_thread(func, ctx)

            extra = self._session_signal_props(ctx, sid_before)
            await self._reply_success(ctx, result, extra_user_props=extra)

        except HandlerError as exc:
            await self._reply_error_ctx(ctx, exc.reason_code, str(exc), body=exc.body)
        except Exception:
            logger.exception(
                "핸들러 미처리 예외: action=%s", ctx.action
            )
            await self._reply_error_ctx(ctx, _RC_UNSPECIFIED, "handler error")

    def _session_signal_props(
        self, ctx: RpcContext, sid_before: Optional[str]
    ) -> Optional[dict]:
        """패턴 E: 핸들러 실행 후 session_id 변화를 응답 User Property로 신호화한다."""
        if self._exclusive_mgr is None:
            return None
        sid_after = self._exclusive_mgr.get_session_id(ctx.client_id)
        if sid_after and sid_after != sid_before:
            return {UP_SESSION_ID: sid_after}  # 새로 획득
        if sid_before and not sid_after:
            return {UP_SESSION_ID: ""}  # 해제됨
        return None

    async def _invoke_streaming(
        self, ctx: RpcContext, func: HandlerFunc, stream_qos: int = 0
    ) -> None:
        """
        스트리밍 구독 핸들러 실행 (패턴 C). 청크를 response 토픽으로 발행한다.

        모든 청크에 ``subscription_id``/``unsubscribe_action``/``is_EOF="false"`` 를
        자동 삽입하고, yields 사이에서 ``cancel_event`` 를 SDK가 자동 체크한다.
        자연 종료/취소 시 EOF(취소면 ``cancel_reason``)를 발행한다.
        """
        sub_id, cancel_event = await self._registry.create(client_id=ctx.client_id)
        ctx.subscription_id = sub_id
        ctx.cancel_event = cancel_event

        try:
            if inspect.isasyncgenfunction(func):
                gen = func(ctx)
                try:
                    async for chunk in gen:
                        await self._reply_chunk(ctx, chunk, stream_qos)
                        if cancel_event.is_set():
                            await gen.aclose()
                            break
                finally:
                    await gen.aclose()
            elif inspect.isgeneratorfunction(func):
                gen = func(ctx)
                try:
                    while True:
                        if cancel_event.is_set():
                            break
                        chunk = await asyncio.to_thread(_safe_next, gen)
                        if chunk is _STREAM_DONE:
                            break
                        await self._reply_chunk(ctx, chunk, stream_qos)
                        if cancel_event.is_set():
                            break
                finally:
                    await asyncio.to_thread(gen.close)
            else:
                raise TypeError(
                    f"subscription=True 핸들러는 generator 또는 async generator이어야 한다: {func}"
                )

            cancel_reason = await self._registry.get_cancel_reason(sub_id)
            await self._reply_eof(ctx, cancel_reason=cancel_reason, stream_qos=stream_qos)

        except HandlerError as exc:
            await self._reply_error_ctx(ctx, exc.reason_code, str(exc), body=exc.body)
        except Exception:
            logger.exception(
                "스트리밍 핸들러 예외: action=%s", ctx.action
            )
            await self._reply_error_ctx(ctx, _RC_UNSPECIFIED, "streaming error")
        finally:
            await self._registry.remove(sub_id)

    async def _reply_chunk(
        self, ctx: RpcContext, chunk: Any, stream_qos: int
    ) -> None:
        """스트림 청크 발행: subscription_id/unsubscribe_action/is_EOF=false 자동 삽입."""
        extra = {
            UP_SUBSCRIPTION_ID: ctx.subscription_id or "",
            UP_UNSUBSCRIBE_ACTION: self._unsubscribe_action,
            UP_IS_EOF: "false",
        }
        await self._reply_success(
            ctx, chunk, is_eof=False, extra_user_props=extra, qos=stream_qos
        )

    def _mirror_qos(self, ctx: RpcContext) -> int:
        """단일/EOF 응답 QoS = 요청 User Property qos (기본 1)."""
        try:
            return int(ctx.request_qos)
        except (TypeError, ValueError):
            return 1

    def _response_expiry(self, ctx: RpcContext, qos: int) -> Optional[int]:
        """QoS 1 단일 응답 Message Expiry = max(1, ceil(timeout - 처리시간))."""
        if qos != 1 or ctx.timeout_s is None:
            return None
        elapsed = _monotonic() - (ctx.received_at or _monotonic())
        remaining = ctx.timeout_s - elapsed
        return max(1, math.ceil(remaining))

    async def _reply_success(
        self,
        ctx: RpcContext,
        result: Any,
        is_eof: bool = False,
        extra_user_props: Optional[dict] = None,
        qos: Optional[int] = None,
    ) -> None:
        """성공 응답 발행. 단일/EOF는 QoS 미러링, 스트림 청크는 stream_qos."""
        if not ctx.response_topic:
            return
        is_chunk = (
            extra_user_props is not None
            and extra_user_props.get(UP_IS_EOF) == "false"
        )
        eff_qos = qos if qos is not None else self._mirror_qos(ctx)

        user_props: list[tuple[str, str]] = [("reason_code", "0")]
        if is_eof:
            user_props.append(("is_EOF", "true"))
        # subscription_id 자동 삽입 (스트림 청크는 extra에 이미 있으므로 제외)
        if not is_chunk and UP_SUBSCRIPTION_ID not in (extra_user_props or {}):
            sid = ctx.subscription_id or str(uuid.uuid4())
            user_props.append((UP_SUBSCRIPTION_ID, sid))
        for k, v in ctx.response_props.items():
            user_props.append((k, str(v)))
        if extra_user_props:
            for k, v in extra_user_props.items():
                user_props.append((k, str(v)))

        expiry = None
        if not is_eof and not is_chunk:
            expiry = self._response_expiry(ctx, eff_qos)

        props = MqttProperties(
            correlation_data=ctx.correlation_id,
            user_properties=user_props,
            message_expiry_interval=expiry,
        )
        raw = encode_payload(result)
        await asyncio.to_thread(
            self._conn.publish, ctx.response_topic, raw, eff_qos, props
        )

    async def _reply_eof(
        self,
        ctx: RpcContext,
        cancel_reason: Optional[str] = None,
        stream_qos: int = 0,
    ) -> None:
        """EOF 응답 발행. cancel_reason 지정 시 User Property에 첨부."""
        extra_props: dict = {UP_SUBSCRIPTION_ID: ctx.subscription_id or ""}
        if cancel_reason:
            extra_props[UP_CANCEL_REASON] = cancel_reason
        await self._reply_success(
            ctx, None, is_eof=True, extra_user_props=extra_props, qos=stream_qos
        )

    async def _reply_error(
        self,
        msg: IncomingMessage,
        reason_code: int,
        detail: str,
    ) -> None:
        """IncomingMessage 기반 오류 응답 발행. QoS는 요청 qos 미러링(기본 1)."""
        if not msg.response_topic:
            return
        eff_qos = 1
        try:
            eff_qos = int(msg.user_props.get(UP_QOS, "1") or "1")
        except (TypeError, ValueError):
            eff_qos = 1
        props = MqttProperties(
            correlation_data=msg.correlation_data,
            user_properties=[
                ("reason_code", str(reason_code)),
                ("error_detail", detail),
            ],
        )
        await asyncio.to_thread(
            self._conn.publish, msg.response_topic, b"", eff_qos, props
        )

    async def _reply_error_ctx(
        self,
        ctx: RpcContext,
        reason_code: int,
        detail: str,
        body: Any = None,
    ) -> None:
        """RpcContext 기반 오류 응답 발행. QoS는 요청 qos 미러링(기본 1)."""
        if not ctx.response_topic:
            return
        eff_qos = self._mirror_qos(ctx)
        props = MqttProperties(
            correlation_data=ctx.correlation_id,
            user_properties=[
                ("reason_code", str(reason_code)),
                ("error_detail", detail),
            ],
        )
        raw = encode_payload(body) if body is not None else b""
        await asyncio.to_thread(
            self._conn.publish, ctx.response_topic, raw, eff_qos, props
        )


_STREAM_DONE = object()


def _safe_next(gen: Any) -> Any:
    """
    동기 generator의 다음 청크를 반환한다. 소진 시 ``_STREAM_DONE`` sentinel을 반환한다.

    ``StopIteration`` 은 asyncio Future 경계를 넘을 수 없으므로(RuntimeError 유발)
    sentinel로 변환하여 ``to_thread`` 와 안전하게 함께 쓴다.
    """
    try:
        return next(gen)
    except StopIteration:
        return _STREAM_DONE


def _monotonic() -> float:
    """단조 시계 (테스트 모킹 용이성을 위해 모듈 함수로 분리)."""
    import time as _time

    return _time.monotonic()


def _parse_int(val: Optional[str]) -> Optional[int]:
    """문자열을 int로. 실패 시 None."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _parse_float(val: Optional[str]) -> Optional[float]:
    """문자열을 float로. 실패 시 None."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
