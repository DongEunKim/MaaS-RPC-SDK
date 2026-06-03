> 원문: [supplement/VISS_ImplementationGuidelines.html](supplement/VISS_ImplementationGuidelines.html) | 출처: COVESA Vehicle Information Service Specification v3.1

# COVESA VISS — 구현 가이드라인

**편집자:** Ulf Bjorkengren (Ford Motor Company), Sergej Saibel (Traton)

Copyright © 2026 COVESA®.

---

## 개정 이력

| 버전 | 날짜 | 작성자 | 개정 이유 |
|------|------|--------|---------|
| 0.5 | 2026-03-25 | Ulf Björkengren (Ford Motor Company) | 초기 문서 레이아웃 |
| 1.0 | 2026-mm-dd | (미정) | 5장 작성 |

---

## 초록

Vehicle Information Service Specification(VISS)은 차량 네트워크 내 제어 장치 센서로부터 수집된 신호를 포함한 차량 정보에 접근하기 위한 서비스다. 이 정보는 COVESA [Hierarchical Information Model](https://github.com/COVESA/hierarchical_information_model)(HIM)에서 정의하는 계층적 트리 형태의 분류 체계로 노출되며, JSON 형식으로 제공된다. VISS 서비스는 차량 내부 또는 이미 오프보딩된 데이터를 사용하는 외부 서버에 호스팅될 수 있다.

VISS는 예측 유지보수, 사용 기반 보험, 차량 관리(플리트 매니지먼트), 실시간 운전 지원 서비스 등 광범위한 사용 사례를 지원한다.

VISS 사양에는 세 가지 부분이 있다: [[CORE]], [[TRANSPORT]], [[PAYLOAD_ENCODING]].

- [[CORE]] 문서는 메시징 계층을 설명한다.
- [[TRANSPORT]] 문서는 일부 전송 프로토콜이 사용하는 CORE 사양의 편차를 설명한다.
- [[PAYLOAD_ENCODING]] 문서는 지원되는 페이로드 인코딩을 설명한다.

이 문서인 VISS 구현 가이드라인은 구현 상호운용성을 보장하기 위해 특정 기능의 구현 방법에 대한 권고사항을 제공한다. 이 문서의 버전 관리는 VISS 사양의 버전 관리와 분리되어 있다. 이전 버전의 VISS에 적용 가능하지 않은 가이드라인 장에 대해서는 해당 장에서 언급된다.

---

## 1. 소개

이 문서는 VISS 사양을 보완하여 사양의 특정 측면 구현 방법에 대한 권고사항을 제공한다. 이러한 측면들은 일반적으로 VISS 인터페이스에 명시적으로 노출되지 않지만, 다른 구현들은 다른 동작을 초래할 수 있어 클라이언트가 서로 다른 구현에서 다른 응답을 받을 수 있다. 이는 클라이언트가 어떤 구현으로도 인터페이스를 사용할 수 있는 상호운용성 달성을 더 복잡하게 만들 수 있다.

---

## 2. 용어

'VISSv3.1'이라는 약어는 이 문서, VISS 버전 3.1 사양을 가리키는 데 사용된다. 'HIM'이라는 약어는 COVESA에서 호스팅하는 ['Hierarchical Information Model'](https://github.com/COVESA/hierarchical_information_model)을 가리킨다. 'VSS'라는 약어는 COVESA에서 호스팅하는 ['Vehicle Signal Specification'](https://github.com/COVESA/vehicle_signal_specification)을 가리킨다. 'WebSocket'은 [W3C WebSocket API](https://www.w3.org/TR/websockets/) 및 RFC6455에 정의된 대로 사용된다.

---

## 3. CAN Bus 특정 오류 보고

*(이 절은 현재 작성 중이다.)*
