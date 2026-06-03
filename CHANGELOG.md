# Changelog

모든 주요 변경 사항은 이 파일에 기록됩니다. [Keep a Changelog](https://keepachangelog.com/ko/1.0.0/) 형식을 따릅니다.

버전 관리는 [Semantic Versioning](https://semver.org/lang/ko/)을 따릅니다.

## [Unreleased]

### Changed
- 프로젝트 디렉토리 구조 재편: 멀티 언어 확장 및 배포 준비
  - `SDK/` → `sdk/python/client/`, `sdk/python/server/`
  - `SDK/examples/` → `examples/python/`
  - `SDK/mosquitto/` → `infra/mosquitto/`
  - `docs/` 정리: 설계 문서(`RPC_DESIGN.md`, `TOPIC_AND_ACL_SPEC.md`)는 루트에, 요구사양서는 `docs/spec/`에 배치
- 패턴 E 독점 세션: VIN별 Lock(`SessionManager`) → 서비스 단위 단일 세션(`ExclusiveSessionManager`, `exclusive_service=True` + `acquire_session()`/`release_session()`)
- 단절 감지: 브로커 수명주기 이벤트(`PresenceMonitor`) → LWT(offline) 기반(`OfflineMonitor`)
- 구 패턴 F(유한 스트리밍)를 패턴 C로 통합 (generator 자연 종료)

### Removed
- 미배포 레거시 제거: `SessionManager`, `PresenceMonitor`, `StreamInterruptedError`,
  `@server.action`/`@server.default` 의 `streaming`/`exclusive`/`acquire_lock`/`release_lock` 인자,
  `MaasServer` 의 `session_idle_timeout`/`lifecycle_topics` 인자 (하위호환 미유지)

### Added
- `protocol/` 언어 중립 명세 (topics.json, error-codes.json, JSON Schema)
- `infra/docker/` Docker Compose 기반 로컬 브로커 환경
- `Makefile` 공통 개발 명령 (`make test`, `make broker`, `make build`)
- `.github/workflows/ci.yml` PR 자동 테스트 (Python 3.10/3.11/3.12)
- `.github/workflows/release.yml` git tag 기반 PyPI 자동 배포

---

## [1.0.0] - 2026-05-29

### Added
- MaaS Client SDK (`maas-client-sdk`): `MaasClient`, `MaasClientAsync`
- MaaS Server SDK (`maas-server-sdk`): `MaasServer`, `RpcContext`
- MQTT 5.0 네이티브 RPC 패턴 A~E 구현
  - A: Liveness (QoS 0, 단일 응답)
  - B: Reliable (QoS 1, 단일 응답)
  - C: Streaming (QoS 1, event × N → EOF)
  - D: Time-bound (QoS 1, Message Expiry 자동 설정)
  - E: Exclusive Session (QoS 1, VIN 단위 독점 Lock)
- `SessionManager`: VIN별 독점 Lock, idle_timeout 자동 해제
- `PresenceMonitor`: 브로커 수명주기 이벤트 → 세션 강제 해제
- `HttpTokenSource`: HTTP 기반 JWT 토큰 갱신
- `PubSubManager`: RPC 외 임의 Pub/Sub 지원
