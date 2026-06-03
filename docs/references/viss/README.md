# COVESA VISS v3.1 사양 (한글 번역)

> 출처: [COVESA Vehicle Information Service Specification v3.1](https://github.com/COVESA/vehicle-information-service-specification)

이 디렉터리에는 COVESA VISS(Vehicle Information Service Specification) v3.1 사양의 한글 번역 마크다운 문서가 포함되어 있다.

---

## 한글 마크다운 문서

| 파일 | 원문 | 설명 |
|------|------|------|
| [01_Core.md](01_Core.md) | [VISSv3.1_Core.html](VISSv3.1_Core.html) | 핵심 사양 — 데이터 모델, 인터페이스, 접근 제어, JSON 스키마 |
| [02_Transport.md](02_Transport.md) | [VISSv3.1_Transport.html](VISSv3.1_Transport.html) | 전송 계층 — WebSocket, HTTPS, MQTT, gRPC, UDS |
| [03_PayloadEncoding.md](03_PayloadEncoding.md) | [VISSv3.1_PayloadEncoding.html](VISSv3.1_PayloadEncoding.html) | 페이로드 인코딩 — Protobuf 스키마, 전송별 실현 |
| [04_ImplementationGuidelines.md](04_ImplementationGuidelines.md) | [supplement/VISS_ImplementationGuidelines.html](supplement/VISS_ImplementationGuidelines.html) | 구현 가이드라인 — 구현 상호운용성 권고사항 |

---

## 원문 HTML 문서

| 파일 | 설명 |
|------|------|
| [VISSv3.1_Core.html](VISSv3.1_Core.html) | VISS Core 원문 HTML |
| [VISSv3.1_Transport.html](VISSv3.1_Transport.html) | VISS Transport 원문 HTML |
| [VISSv3.1_PayloadEncoding.html](VISSv3.1_PayloadEncoding.html) | VISS Payload Encoding 원문 HTML |
| [supplement/VISS_ImplementationGuidelines.html](supplement/VISS_ImplementationGuidelines.html) | VISS 구현 가이드라인 원문 HTML |
| [index.html](index.html) | VISS 사양 인덱스 페이지 |

---

## 문서 구조

```
docs/references/viss/
├── README.md                          ← 이 파일 (인덱스)
├── 01_Core.md                         ← VISS Core 한글 번역
├── 02_Transport.md                    ← VISS Transport 한글 번역
├── 03_PayloadEncoding.md              ← VISS Payload Encoding 한글 번역
├── 04_ImplementationGuidelines.md     ← VISS 구현 가이드라인 한글 번역
├── VISSv3.1_Core.html                 ← 원문 HTML
├── VISSv3.1_Transport.html            ← 원문 HTML
├── VISSv3.1_PayloadEncoding.html      ← 원문 HTML
├── images/                            ← 사양 이미지
├── resources/                         ← JSON Schema, Protobuf, OpenAPI 파일
├── supplement/
│   └── VISS_ImplementationGuidelines.html
└── index.html
```

---

## 주요 섹션 요약

### 01_Core.md — VISS Core 사양

- **데이터 모델**: HIM 기반 트리 구조, 포레스트 개념, 주소 지정 방식
- **인터페이스**: Read, Update, Subscribe, Unsubscribe, Subscription 메서드
- **필터**: paths, timebased, range, change, curvelog, history, metadata
- **접근 제어**: OAuth2.0 기반 AGT/AT 토큰 모델, 단기/장기 플로우
- **동의 지원**: ECF(External Consent Framework) 통합
- **부록**: JSON Schema, 서버 기능 트리, 파일 전송, 데이터 압축, 순서 어긋난 요청

### 02_Transport.md — VISS Transport 사양

- **공통 정의**: 상태 코드, 인라인 오류 보고, 인가 방식
- **WebSocket**: 세션 관리, 전체 메시지 예시(Read/Update/Subscribe/Unsubscribe)
- **HTTPS**: HTTP GET/POST 매핑, 인가 헤더 방식
- **MQTT**: 애플리케이션 수준 프로토콜, VID/Vehicle 토픽 구조
- **gRPC**: Protobuf 기반 직렬화
- **UDS**: Unix Domain Socket 사용 방법

### 03_PayloadEncoding.md — VISS Payload Encoding 사양

- **Protobuf 스키마**: VISS 메시지 페이로드 전체 .proto 정의
- **전송별 실현**: gRPC, WebSocket(포트/서브프로토콜), MQTT(토픽 명명)
- **JSON 스키마 기반 인코딩**: OpenAPI/AsyncAPI 활용 방법

### 04_ImplementationGuidelines.md — 구현 가이드라인

- **목적**: 구현 간 상호운용성 보장을 위한 권고사항
- **CAN Bus 특정 오류 보고**: (작성 중)
