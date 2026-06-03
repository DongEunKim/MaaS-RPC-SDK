# SDK

MQTT 5.0 기반 **MaaS Client SDK** 및 **MaaS Server SDK** (Python 3.10+).

## 패키지

| 패키지 | 경로 | 설명 |
|--------|------|------|
| **maas-client-sdk** | [`python/client/`](python/client/) | 클라이언트 앱용 — 사용법은 [README](python/client/README.md) |
| **maas-server-sdk** | [`python/server/`](python/server/) | 서비스 구현용 — 사용법은 [README](python/server/README.md) |

별도 Envelope 프로토콜 없이 MQTT 5.0 `Response Topic`, `Correlation Data`, `User Properties`를 사용한다.

## 설치 (개발)

저장소 루트에서:

```bash
pip install -r requirements.txt
```

또는 패키지별 editable 설치:

```bash
pip install -e sdk/python/client[dev]
pip install -e sdk/python/server[dev]
```

## 문서

- [maas-client-sdk 가이드](python/client/README.md)
- [maas-server-sdk 가이드](python/server/README.md)
- [RPC 설계](../docs/RPC_DESIGN.md) — 패턴 A~E, 내부 메커니즘
- [토픽·ACL 규격](../docs/TOPIC_AND_ACL_SPEC.md) — WMT/WMO 토픽 구조

## 예제

로컬 브로커·통합 테스트는 [examples/python/README.md](../examples/python/README.md)를 참고한다.
