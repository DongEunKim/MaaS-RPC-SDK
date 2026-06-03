"""
maas-client-sdk: MQTT 5.0 기반 MaaS RPC 클라이언트 SDK.

기본 인터페이스(동기):
    from maas_client import MaasClient

고급 인터페이스(비동기):
    from maas_client import MaasClientAsync
"""

from .auth import HttpTokenSource, TokenSource, TokenProvider
from .client import MaasClient, SyncSubscription, SyncServerWatcher, SyncResponse
from .client_async import MaasClientAsync
from .models import (
    Response,
    RpcResponse,
    StreamEvent,
    Message,
    Session,
    Subscription,
    ServerWatcher,
)
from .exceptions import (
    MaasError,
    ConnectionError,
    RpcTimeoutError,
    RpcExpiredError,
    RpcServerError,
    NotAuthorizedError,
    ServerBusyError,
    SubscriptionCancelledError,
    ServerOfflineError,
)
from . import topics

__all__ = [
    "HttpTokenSource",
    "TokenSource",
    "TokenProvider",
    "MaasClient",
    "MaasClientAsync",
    "SyncSubscription",
    "SyncServerWatcher",
    "SyncResponse",
    "Response",
    "RpcResponse",
    "StreamEvent",
    "Message",
    "Session",
    "Subscription",
    "ServerWatcher",
    "MaasError",
    "ConnectionError",
    "RpcTimeoutError",
    "RpcExpiredError",
    "RpcServerError",
    "NotAuthorizedError",
    "ServerBusyError",
    "SubscriptionCancelledError",
    "ServerOfflineError",
    "topics",
]

__version__ = "1.0.0"
