# MaaS RPC 개념설계서

> 문서 유형: 개념설계서
> 독자: 모든 개발자 (신규 합류 포함)
> 이 문서의 위치: `README` → `docs/README`(문서 지도) → **본 문서** → 깊은 개념·사양·API 문서
>
> 본 문서는 시스템의 **멘탈 모델을 한 번에 세우고**, 그다음 어느 문서로 갈지 안내하는 허브다.
> 설치 방법(→ `README`), 실행 가능한 API(→ SDK README·examples), 내부 자료구조(→ 상세설계사양서)는
> 담지 않고 해당 문서로 연결한다.

---

## 1. 빠른 이해 (Quick Overview)

이 섹션은 MaaS RPC SDK를 처음 접하는 개발자를 위한 최소 진입점이다.

### 시스템이 하는 일

웹/앱 클라이언트가 MQTT 5.0 브로커를 통해 엣지(차량, 장비 등)의 서비스를 **원격 함수 호출(RPC)** 방식으로 제어·조회한다.

```
[Web/App]  --WSS+JWT-->  [MQTT 5.0 브로커]  --TLS+X.509-->  [Edge Service]
 maas-client-sdk                                              maas-server-sdk
```

### 양쪽의 개념 흐름

- **클라이언트**: 접속 정보와 대상(`ThingType`/`Service`/`VIN`)으로 SDK를 만들고 연결한 뒤, `call(action, params)`로 호출하면 결과를 돌려받는다. 토픽·상관관계 식별자·응답 라우팅은 SDK가 숨긴다.
- **서버**: `action` 이름에 핸들러를 등록(`@server.action`)하고 실행하면, SDK가 들어온 요청을 해당 핸들러로 라우팅하고 반환값을 응답 토픽으로 자동 발행한다.

> 실행 가능한 전체 예제와 API 시그니처는 [클라이언트 SDK README](../sdk/python/client/README.md) ·
> [서버 SDK README](../sdk/python/server/README.md) · [examples](../examples/python/README.md) 참고.

### 토픽 패턴 한눈에 보기

| 방향 | 토픽 패턴 |
|------|-----------|
| 클라이언트 → 서비스 | `WMT/{ThingType}/{Service}/{VIN}/{ClientId}/request` |
| 서비스 → 클라이언트 | `WMO/{ThingType}/{Service}/{VIN}/{ClientId}/response` |

클라이언트 구독: `WMO/+/+/+/{ClientId}/response` (단 1개, 연결 시 자동 등록)

---

## 2. 문서 목적 및 범위

본 문서는 MaaS RPC 프레임워크의 **개념·원칙·설계 배경**을 정의하고, 깊은 문서로의 진입점 역할을 한다.

- 대상 독자: 신규 개발자, 아키텍트, 연동 담당자
- 포함 내용: 설계 배경, 핵심 원칙, 개념 아키텍처, 메커니즘 *개념*, RPC 패턴 요약, 다음 읽을 문서 안내
- 제외 내용(각 주인 문서로 연결):
  - 설치·저장소 구조 → [README](../README.md)
  - API 상세 명세·실행 코드 → SDK README, examples
  - 패턴별 시퀀스 다이어그램 → [RPC_DESIGN.md](RPC_DESIGN.md)
  - 토픽·ACL·Reason Code 규격 → [TOPIC_AND_ACL_SPEC.md](TOPIC_AND_ACL_SPEC.md)
  - 내부 구현 구조·자료구조 → [SDK 상세설계사양서](spec/SDK%20%EC%83%81%EC%84%B8%EC%84%A4%EA%B3%84%EC%82%AC%EC%96%91%EC%84%9C.md)

---

## 3. 설계 배경 — 왜 MQTT 5.0 네이티브 RPC인가

### 3.1 문제: 엣지 서비스에 대한 안정적 RPC

엣지 장비는 TCP 포트를 직접 노출하기 어렵다. 방화벽, NAT, 불안정한 모바일 네트워크가 직접 연결을 막는다. 그래서 MQTT 브로커를 중간에 두는 방식이 일반적이다.

그러나 브로커를 쓰면 새 문제가 생긴다.

- 요청에 대한 응답을 어떻게 구분하는가?
- 여러 클라이언트가 동시에 요청할 때 응답이 섞이지 않는가?
- 스트리밍 데이터를 어떻게 처리하는가?
- 단 하나의 클라이언트만 명령 가능한 세션은 어떻게 구현하는가?

### 3.2 해결: HTTP Envelope 없이 MQTT 5.0 Properties만 사용

MQTT 5.0은 메시지에 추가 메타데이터를 붙이는 **Properties** 기능을 제공한다. MaaS RPC SDK는 이 Properties만으로 RPC의 모든 메커니즘을 처리한다.

별도의 HTTP Envelope 게이트웨이, 커스텀 프레이밍 프로토콜, 중앙 조정 서버가 필요 없다.

---

## 4. 핵심 설계 원칙

| 원칙 | 내용 |
|------|------|
| **Envelope 없음** | MQTT 5.0 Properties(`Response Topic`, `Correlation Data`, `User Property`)로만 RPC 메타데이터 처리 |
| **토픽 은닉** | 애플리케이션 코드는 WMT/WMO 전체 경로를 몰라도 된다. SDK가 자동 생성 |
| **단일 구독** | 클라이언트는 `WMO/+/+/+/{ClientId}/response` 단 1개만 구독. 모든 응답(단일·스트리밍)을 이 토픽으로 수신 |
| **Clean Start = True** | 재연결 시 stale 응답이 클라이언트에 도달하는 것을 방지 |
| **동기 우선** | 동기 Facade가 기본. asyncio 환경용 비동기 클래스도 제공 |
| **어댑터 추상화** | 서버 SDK는 전송 계층을 어댑터로 추상화해 paho-mqtt(CCU)와 Greengrass IPC(CGU)를 교체 가능하게 지원 |

---

## 5. 시스템 개념 아키텍처

```mermaid
graph LR
    subgraph 클라이언트 영역
        C[Web / App]
    end

    subgraph 클라우드 영역
        B[MQTT 5.0 브로커\nAWS IoT Core]
    end

    subgraph 엣지 영역
        CGU[CGU RPC Server\nGreengrass IPC 어댑터]
        EMQX[CGU 로컬 브로커\nEMQX]
        CCU[CCU RPC Server\npaho-mqtt 어댑터]
    end

    C -- "WSS + JWT" --> B
    B -- "Greengrass IPC" --> CGU
    B -- "MQTT Bridge" --> EMQX
    EMQX -- "mTLS + X.509" --> CCU
```

### 두 가지 엣지 서버 시나리오

전송 계층만 다르고 **서비스 구현 코드(핸들러 등록·실행)는 동일**하다는 것이 핵심이다.

| 항목 | CGU (Greengrass Component) | CCU (Client Device) |
|------|---------------------------|---------------------|
| 연결 방식 | Greengrass IPC (Unix Socket) | paho-mqtt → EMQX (mTLS) |
| 전송 계층 | Greengrass IPC 어댑터 | paho-mqtt 어댑터 |
| 서비스 핸들러 코드 | **동일** | **동일** |

> 어댑터를 코드에서 어떻게 선택하는지는 [서버 SDK README](../sdk/python/server/README.md) 참고.

---

## 6. 핵심 메커니즘 개요

세부 시퀀스·예외 흐름은 [RPC_DESIGN.md](RPC_DESIGN.md), 토픽·Property 규격은
[TOPIC_AND_ACL_SPEC.md](TOPIC_AND_ACL_SPEC.md)에 있다. 여기서는 개념만 본다.

### 6.1 WMT/WMO 토픽 체계

| 방향 | 접두어 | 의미 |
|------|--------|------|
| 클라이언트 → 서비스 | `WMT` | Web Mobile Terminated (요청) |
| 서비스 → 클라이언트 | `WMO` | Web Mobile Oriented (응답) |

토픽 구조:

```
요청:  WMT/{ThingType}/{Service}/{VIN}/{ClientId}/request
응답:  WMO/{ThingType}/{Service}/{VIN}/{ClientId}/response
```

- `{ThingType}`: 장비 타입 (예: CGU, SDM)
- `{Service}`: 서비스 이름 (예: viss, diagnostics)
- `{VIN}`: 대상 장비 식별자
- `{ClientId}`: 응답 라우팅용 클라이언트 식별자 (UUID 권장)

### 6.2 Correlation Data 기반 요청-응답 매핑

여러 RPC 요청이 동시에 진행 중일 때 응답이 섞이지 않도록 매핑한다.

1. 요청 시 SDK가 고유 식별자(UUID)를 생성해 요청 메시지의 `Correlation Data` Property로 첨부한다.
2. 서버는 응답 메시지에 동일한 식별자를 그대로 포함한다.
3. 클라이언트 SDK는 동일 식별자로 대기 중인 요청을 찾아 해당 응답으로 매칭한다.

개발자가 직접 식별자를 관리할 필요가 없다. (대기 자료구조 등 구현 방식은 상세설계사양서 참고.)

### 6.3 Response Topic 기반 라우팅

클라이언트가 요청 메시지의 `Response Topic` Property에 응답 받을 토픽을 명시한다.

- 서버는 MQTT 5.0 Property에서 `Response Topic`을 읽어 응답을 발행한다.
- 페이로드 안에 응답 주소를 넣지 않으므로 페이로드 구조가 단순하다.
- SDK가 자동으로 삽입하고 처리한다.

---

## 7. RPC 패턴 분류 (A~E) 요약표

| 패턴 | 이름 | QoS | 핵심 특징 |
|------|------|-----|-----------|
| A | Best-Effort 조회 | 0 | 빠른 상태 조회. Message Expiry 생략. 유실 시 재시도 가능 |
| B | 신뢰성 제어 | 1 | 하드웨어 제어 등 결과 보장 명령. 재전송 보장 |
| C | 스트리밍 구독 | 0 (서버가 1 선택 가능) | 패턴 A와 동일한 요청 형태. 서버가 `is_EOF=false`로 스트림 선언. 유한·무한 모두 지원. 클라이언트 취소·서버 강제 취소·자연 종료 |
| D | 시한성 제어 | 1 | `valid_for` 유효기간 초과 시 **미실행 보장** (요청 Message Expiry 자동 설정) |
| E | 독점 세션 | 1 | **서비스 단위** 독점. 타 클라이언트 요청 시 `0x8A(Server Busy)` 반환. 단절 시 LWT로 자동 해제 |

패턴별 시퀀스·전제/사후조건·예외 흐름은 [RPC_DESIGN.md](RPC_DESIGN.md) 참고. (큐 선점용 패턴 G는 예약 — 현재 구현 범위 밖.)

---

## 8. SDK 구성 및 역할

| SDK | 역할 | 비고 |
|-----|------|------|
| **maas-client-sdk** | RPC를 **호출하는** 쪽. 웹 앱, 관리 도구, 테스트 스크립트 등 | 동기 Facade가 기본, asyncio 환경용 비동기 클래스도 제공 |
| **maas-server-sdk** | RPC를 **처리하는** 쪽. 엣지 서비스 | 전송 계층(paho-mqtt / Greengrass IPC)을 어댑터로 선택 |

클래스명, 생성자 인자, `call`/`stream`/세션 API, 어댑터 선택 옵션 등 구체적인 사용법은
[클라이언트 SDK README](../sdk/python/client/README.md)와 [서버 SDK README](../sdk/python/server/README.md)에 있다.

---

## 9. 다음에 무엇을 읽을까

본 문서로 멘탈 모델을 세웠다면, **목적에 따라** 아래로 이동한다.
(문서 전체 지도와 계층은 [docs/README.md](README.md)에 있다.)

| 하고 싶은 것 | 읽을 문서 |
|---|---|
| 패턴별 동작·시퀀스·예외 흐름을 알고 싶다 | [RPC_DESIGN.md](RPC_DESIGN.md) |
| 토픽 구조·ACL·Reason Code·인증 정책을 확인한다 | [TOPIC_AND_ACL_SPEC.md](TOPIC_AND_ACL_SPEC.md) |
| 연결 수명주기·재연결·세션 해제 정책을 본다 | [CONNECTION_MANAGEMENT.md](CONNECTION_MANAGEMENT.md) |
| 클라이언트로 RPC를 호출한다 | [클라이언트 SDK README](../sdk/python/client/README.md) |
| 엣지 서비스(서버)를 구현한다 | [서버 SDK README](../sdk/python/server/README.md) |
| 바로 실행해 본다 | [examples](../examples/python/README.md) |
| SDK 요구사항(공개 API 계약)을 본다 | [SDK 요구사양서](spec/SDK%20%EC%9A%94%EA%B5%AC%EC%82%AC%EC%96%91%EC%84%9C.md) |
| 내부 구현 구조·자료구조를 본다 | [SDK 상세설계사양서](spec/SDK%20%EC%83%81%EC%84%B8%EC%84%A4%EA%B3%84%EC%82%AC%EC%96%91%EC%84%9C.md) |
