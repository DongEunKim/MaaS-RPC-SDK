# 문서

MaaS RPC SDK 프로젝트 이해 및 연동에 필요한 문서 목록.

## 빠른 시작

- SDK를 처음 접하는 경우 → [OVERVIEW.md](OVERVIEW.md)부터 시작한다.
- 토픽·ACL 규격을 확인하려면 → [TOPIC_AND_ACL_SPEC.md](TOPIC_AND_ACL_SPEC.md).
- 패턴별 설계 및 시퀀스 다이어그램이 필요하면 → [RPC_DESIGN.md](RPC_DESIGN.md).
- 바로 실행해 보려면 → [examples/python/README.md](../examples/python/README.md).

## 문서 계층

문서는 **개념설계 → SDK 요구사양 → SDK 상세설계 → 코드** 순으로 도출된다. 개념설계 문서는
프로토콜·토픽·패턴·정책의 *개념*만 다루며 SDK 코드를 언급하지 않는다.

| 단계 | 유형 | 문서 | 독자 |
|------|------|------|------|
| 개념설계 | 개념설계서 | [OVERVIEW.md](OVERVIEW.md) | 모든 개발자 |
| 개념설계 | 기능정의서 | [RPC_DESIGN.md](RPC_DESIGN.md) | 개발자 |
| 개념설계 | 인터페이스 정의서 | [TOPIC_AND_ACL_SPEC.md](TOPIC_AND_ACL_SPEC.md) | 개발자, QA |
| 개념설계 | 연결 관리 정책서 | [CONNECTION_MANAGEMENT.md](CONNECTION_MANAGEMENT.md) | SDK 구현·연동 |
| SDK 요구사양 | SDK 요구사양서 | [spec/SDK 요구사양서.md](spec/SDK%20%EC%9A%94%EA%B5%AC%EC%82%AC%EC%96%91%EC%84%9C.md) | PM, 아키텍트 |
| SDK 상세설계 | SDK 상세설계사양서 | [spec/SDK 상세설계사양서.md](spec/SDK%20%EC%83%81%EC%84%B8%EC%84%A4%EA%B3%84%EC%82%AC%EC%96%91%EC%84%9C.md) | SDK 구현·유지보수 |

## 상위 계층 시스템 요구 (참고 전용)

이 SDK보다 **상위 계층**의 시스템 요구사양이다. SDK 설계의 입력 배경으로 참고만 하며, 편집 대상이 아니다.

| 문서 | 독자 |
|------|------|
| [references/sdm/04. MaaS RPC 프레임워크 시스템요구사양서.md](references/sdm/04.%20MaaS%20RPC%20%ED%94%84%EB%A0%88%EC%9E%84%EC%9B%8C%ED%81%AC%20%EC%8B%9C%EC%8A%A4%ED%85%9C%EC%9A%94%EA%B5%AC%EC%82%AC%EC%96%91%EC%84%9C.md) | 아키텍트, PM |
| [references/sdm/07. Telemetry Gateway 시스템요구사양서.md](references/sdm/07.%20Telemetry%20Gateway%20%EC%8B%9C%EC%8A%A4%ED%85%9C%EC%9A%94%EA%B5%AC%EC%82%AC%EC%96%91%EC%84%9C.md) | 인프라, 플랫폼 팀 |
| [references/viss/](references/viss/) | VISS 연동 개발자 |

## SDK 패키지

| 패키지 | 경로 | 용도 |
|--------|------|------|
| `maas-client-sdk` | [sdk/python/client/README.md](../sdk/python/client/README.md) | RPC 클라이언트 SDK |
| `maas-server-sdk` | [sdk/python/server/README.md](../sdk/python/server/README.md) | RPC 서버 SDK |

예제 실행 방법은 [examples/python/README.md](../examples/python/README.md) 참고.
