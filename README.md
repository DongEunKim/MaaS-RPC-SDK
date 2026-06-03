# MaaS MQTT RPC

MQTT 5.0 기반 **MaaS(Machine as a Service) RPC** 설계 및 Python SDK 저장소입니다.  
별도 Envelope 게이트웨이 없이 `WMT` / `WMO` 토픽과 MQTT 5.0 Properties로 요청-응답·스트리밍을 처리합니다.

## 프로젝트 구조

```
├── docs/            # 사양·가이드 (docs/README.md)
├── sdk/python/
│   ├── client/      # install.sh, maas_client/
│   └── server/      # install.sh, maas_server/
├── examples/python/ # 예제·브로커 디버깅 스크립트
├── infra/           # Docker·Mosquitto 로컬 브로커 환경
├── protocol/        # 언어 중립 명세 (topics.json, 스키마)
└── tests/e2e/       # E2E 통합 테스트
```

## 개발 환경

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 문서

- [문서 목록](docs/README.md)
- [RPC 설계](docs/RPC_DESIGN.md) — 패턴 A~E
- [토픽·ACL](docs/TOPIC_AND_ACL_SPEC.md)
- [SDK 개요](sdk/README.md)
- [예제·로컬 브로커](examples/python/README.md)

## 진행 작업

[TODO.md](TODO.md) 참고.
