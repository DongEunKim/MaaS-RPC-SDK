> 원문: [VISSv3.1_Transport.html](VISSv3.1_Transport.html) | 출처: COVESA Vehicle Information Service Specification v3.1

# COVESA VISS 버전 3.1 — Transport

**편집자:** Ulf Bjorkengren (Ford Motor Company), 이원석(Wonsuk Lee) (한국전자통신연구원 ETRI)

Copyright © 2026 COVESA®.

---

## 초록

Vehicle Information Service Specification(VISS)은 차량 네트워크 내 제어 장치 센서로부터 수집된 신호를 포함한 차량 정보에 접근하기 위한 서비스다. 이 정보는 COVESA [Hierarchical Information Model](https://github.com/COVESA/hierarchical_information_model)(HIM)에서 정의하는 계층적 트리 형태의 분류 체계로 노출되며, JSON 형식으로 제공된다.

이 사양은 세 부분으로 구성된다: [[CORE]], [[PAYLOAD ENCODING]], TRANSPORT. 이 문서인 VISS 버전 3.1 TRANSPORT 사양은 VISSv3.1 전송 프로토콜과 메시징 계층이 각 전송 방식에 매핑되는 방법을 설명한다. 함께 제공되는 [[CORE]] 사양은 메시징 계층을 설명하고, [[PAYLOAD ENCODING]] 사양은 기본 JSON 페이로드 형식으로부터/으로의 페이로드 인코딩을 설명한다.

---

## 1. 소개

이 사양은 [[CORE]]에 정의된 메시지 페이로드가 다양한 전송 프로토콜과 함께 어떻게 사용되는지에 대한 예시를 제공한다. WebSocket 프로토콜(RFC6455)은 JSON 기본 페이로드 형식이 직접 어떻게 사용되는지를 보여주는 예시로 사용된다. 이어서 기본 페이로드 형식에 예외 또는 변환을 도입하는 다른 전송 프로토콜의 예시가 제공된다.

VISSv3.1은 기본 페이로드 형식의 변환 없이 전달하거나 인코딩된 버전을 전달할 수 있는 모든 전송 프로토콜의 사용을 허용한다. 이 문서에서 편차를 정의하는 전송 프로토콜:

| 프로토콜 이름 | 참조 |
|------------|------|
| HTTP | RFC9112 |
| MQTT | [MQTT] |
| gRPC | [gRPC](https://grpc.io/) |

WebSocket 프로토콜은 편차를 도입하지 않는 프로토콜의 예시로 포함되므로 위 목록에 나열되지 않는다.

---

## 2. 용어

'VISSv3.1'이라는 약어는 이 문서, VISS 버전 3.1 사양을 가리키는 데 사용된다. 'HIM'이라는 약어는 COVESA에서 호스팅하는 ['Hierarchical Information Model version 1.0 Vehicle data profile'](https://github.com/COVESA/hierarchical_information_model)을 가리킨다. 'VSS'는 COVESA가 관리하는 ['Vehicle Signal Specification'](https://github.com/COVESA/vehicle_signal_specification)을 가리킨다. 'WebSocket'은 [W3C WebSocket API](https://www.w3.org/TR/websockets/) 및 RFC6455에 정의된 대로 사용된다.

---

## 3. 전송 공통 정의

이 절은 모든 전송 프로토콜에 공통이어야 하는 기능을 정의한다.

### 3.1 상태 코드

이 사양을 구현하는 서버는 아래 표에 나열된 오류 코드, 이유, 설명을 모든 지원 전송 프로토콜에서 지원해야 한다. 서버는 오류 설명을 동적으로 대체할 수 있다.

| 오류 번호(코드) | 오류 이유 | 오류 설명 |
|--------------|---------|----------|
| 400 (Bad Request) | bad_request | The request is malformed |
| 400 (Bad Request) | invalid_data | Data in the request is invalid |
| 401 (Unauthorized) | invalid_token | Access token is invalid |
| 403 (Forbidden) | forbidden_request | The server refuses to carry out the request |
| 404 (Not Found) | unavailable_data | The requested data was not found |
| 408 (Request Timeout) | request_timeout | Subscribe duration limit exceeded |
| 429 (Too Many Requests) | too_many_requests | Rate-limiting due to too many requests |
| 502 (Bad Gateway) | bad_gateway | The upstream server response was invalid |
| 503 (Service Unavailable) | service_unavailable | The server is temporarily unable to handle the request |
| 504 (Gateway Timeout) | gateway_timeout | The upstream server took too long to respond |

#### 3.1.1 인라인 오류 보고

클라이언트는 여러 신호를 get 또는 subscribe 하기 위해 하나의 요청을 발행할 수 있다. 요청된 신호 중 하나 이상이 일시적으로 사용 불가능한 경우 서버에는 두 가지 응답 옵션이 있다:

- 오류 메시지: 404 - unavailable_data
- 인라인 오류 보고

오류 메시지를 반환하면 클라이언트는 사용 가능한 신호를 포함한 모든 신호를 받지 못한다. 반면, 인라인 오류 보고는 클라이언트가 사용 가능한 모든 신호를 수신하면서 사용 불가능한 신호에 대해 오류를 표시할 수 있게 한다.

인라인 오류 보고에서는 누락된 값에 대해 서버가 값을 다음 문자열로 대체한다: `"viss-inline:Data-not-available"`

> 참고: "viss-inline:" 접두사는 일반 문자열 값에 사용되어서는 안 된다.

인라인 오류 보고 예시:

```json
{
    "action": "get",
    "data": [
        {
            "dp": {
                "ts": "2025-02-04T15:48:43.739Z",
                "value": "viss-inline:Data-not-available"
            },
            "path": "Vehicle.ADAS.ABS.IsEnabled"
        },
        {
            "dp": {
                "ts": "2025-02-04T15:48:43.739Z",
                "value": "true"
            },
            "path": "Vehicle.ADAS.ABS.IsError"
        }
    ],
    "requestId": "237",
    "ts": "2025-02-04T15:48:43.739Z"
}
```

인라인 오류 보고는 접근 제어가 요청에 필요한 경우에 사용되어서는 안 된다. 목적과 연관된 데이터 집합의 계약이 축소된 데이터 집합으로는 이행될 수 없기 때문이다.

#### 3.1.2 공통 오류 시나리오

| 오류 시나리오 | 오류 메시지 | 설명 |
|-------------|----------|------|
| 잘못된 데이터 유형 | 400 - invalid_data - Incorrect data type | 트리의 데이터 유형과 불일치 |
| 신호 현재 사용 불가 | 404 - unavailable_data - Data temporarily unaccessible | 차량이 현재 신호를 제공하지 않음 |
| 알 수 없는 신호 | 404 - unavailable_data - Data is unknown | 신호가 트리에 없음 |
| 잘못된 필터 사용 | 400 - bad_request - Missing or invalid filter | 누락되거나 유효하지 않은 필터 |
| 잘못된 필터 | 400 - bad_request - Incorrect filter | 요청 메서드에 허용되지 않는 필터 유형 |
| 알 수 없는 구독 ID | 404 - unavailable_data - Unknown subscription Id | 활성 구독에서 구독 ID를 찾을 수 없음 |
| 지원되지 않는 기능 ID | 404 - unavailable_data - Unsupported feature | 해당 기능이 서버 기능이 아님 |
| 잘못된 접근 권한 | 401 - invalid_token - Access token is invalid | 접근 권한이 만료되었거나 유효하지 않음 |

#### 3.1.3 400 Bad Request 오류 설명

400 Bad Request 오류 코드 및 관련 이유는 JSON 스키마와 관련된 오류에 사용되어야 한다. 서버는 다음 오류 설명 중 하나로 동적으로 대체할 수 있다:

- Missing or invalid `action`
- Missing or invalid `path`
- Missing or invalid `filter`
- Missing or invalid `value`

#### 3.1.4 400 Invalid Data 오류 설명

400 Invalid Data 오류 코드 및 이유는 JSON 스키마로 다루지 않지만 HIM 속성에 의해 정의된 규칙을 위반하는 오류에 사용되어야 한다. 서버는 다음으로 동적 대체 가능:

- Update of a sensor is not supported
- Requested action on a branch is not supported
- Data value outside limit
- Incorrect data type

#### 3.1.5 401 Unauthorized 오류 설명

401 Unauthorized 오류 코드 및 이유는 접근 제어 검증과 관련된 오류에 사용되어야 한다. 서버는 다음으로 동적 대체 가능:

- Access token has expired
- Access token is missing

#### 3.1.6 404 Not Found 오류 설명

404 Not Found 오류 코드 및 이유는 서버가 요청된 데이터에 접근할 수 없을 때 사용되어야 한다. 서버는 다음으로 동적 대체 가능:

- Data temporarily unaccessible
- Data is unknown

### 3.2 전송 페이로드

페이로드는 JSON 형식이어야 한다. 기본 페이로드 형식에 대한 세부 사항은 [[CORE]] 사양의 부록 A. JSON Schema를 참조한다.

### 3.3 인가

신호가 인가를 필요로 하는 경우, 클라이언트는 서버에 Access Token을 제공하여 요청된 서비스에 대한 접근 권한이 있음을 확인해야 한다 ([[CORE]] 사양 참조).

- HTTP 요청의 경우: 토큰은 `Authorization` 헤더에 포함되어야 한다.
- 기본 페이로드 형식을 사용하는 전송 프로토콜의 경우: 페이로드의 선택적 `authorization` 속성이 사용되어야 한다.

---

## 4. 전송 프로토콜

VISSv3.1 사양은 HTTP, WebSocket, gRPC, UDS, MQTT를 지원 전송 프로토콜로 나열하지만, 이것이 최종 목록은 아니다. 이 사양은 특정 전송 프로토콜 사용을 의무화하지 않지만, 나열된 전송 프로토콜 중 최소 하나는 지원해야 한다.

### 4.1 Secure WebSocket

WebSocket 프로토콜은 기본 페이로드 형식에 편차를 적용하지 않는 프로토콜의 예시로 사용된다. WebSocket은 요청과 응답 메시지 간의 논리적 연결을 제공하지 않으므로, [[CORE]] 사양에 설명된 대로 "requestId" 키 이름을 가진 키-값 쌍이 데이터 구성 요소에 추가되어야 한다. 또한 WebSocket은 명시적인 메서드를 정의하지 않으므로 "action" 키-값 쌍도 포함되어야 한다.

#### 4.1.1 세션 수명 관리

##### 초기화

클라이언트 애플리케이션이 웹 런타임에서 실행되는 HTML 애플리케이션이거나 브라우저에서 실행되는 웹 페이지인 경우, WebSocket 인스턴스는 기본적으로 인스턴스화되거나 '표준 준수' WebSocket JavaScript 라이브러리를 사용하여 생성될 수 있다.

VISSv3 서버 인스턴스의 위치는 구성 또는 레지스트리를 통해 처리될 수 있다. 이 사양에서 'wwwVISSv3' 호스트 이름이 예시로 사용된다.

서브 프로토콜 이름은 'VISSv3'여야 한다. 서브 프로토콜 버전은 정확히 하나의 VISS 서버 사양 버전과 연관된다.

```javascript
var vehicle = new WebSocket("wss://wwwVISSv3:6443", "VISSv3");
```

모든 WebSocket 통신은 'wss'(WebSocket Secure)를 사용해야 한다. 암호화되지 않은 통신은 지원되지 않으므로 서버는 'ws' 연결 요청을 거부해야 한다.

여러 WebSocket 연결이 설정된 경우 각 연결은 독립적으로 관리되어야 한다. 예를 들어 하나의 연결을 통해 생성된 구독은 해당 연결을 통해서만 이벤트를 트리거할 수 있으며, 클라이언트는 구독 취소에도 같은 연결을 사용해야 한다.

##### 종료

WebSocket 연결은 클라이언트 또는 서버 중 하나가 WebSocket 인스턴스에서 'close()' 메서드를 호출함으로써 종료될 수 있다.

```javascript
// WebSocket 열기
var vehicle = new WebSocket("wss://wwwVISSv3:6443", "VISSv3");
// ...
// WebSocket 닫기
vehicle.close();
```

VISSv3 서버는 서버 정의 타임아웃 기간 내에 클라이언트로부터 요청을 받지 못하면 연결을 종료할 수 있다.

#### 4.1.2 전송 메시지

##### Read (읽기)

클라이언트는 서버에 `getRequest` 메시지를 전송하여 하나 이상의 차량 신호 값을 가져올 수 있다.

**getRequest 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| getRequest | action | Action | 예 |
| | path | string | 예 |
| | filter | string | 선택적 |
| | authorization | string | 선택적 |
| | dc | string | 선택적 |
| | requestId | string | 예 |

**getSuccessResponse 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| getSuccessResponse | action | Action | 예 |
| | requestId | string | 예 |
| | data | object/array | 예 |
| | ts | string | 예 |

**getErrorResponse 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| getErrorResponse | action | Action | 예 |
| | requestId | string | 예 |
| | error | Error | 예 |
| | ts | string | 예 |

**예시:**

요청:
```json
{
  "action": "get",
  "path": "Vehicle.Powertrain.CombustionEngine.RPM",
  "requestId": "8756"
}
```

성공 응답:
```json
{
  "action": "get",
  "requestId": "8756",
  "data": {
    "path": "Vehicle.Powertrain.CombustionEngine.RPM",
    "dp": {"value": "2372", "ts": "2020-04-15T13:37:00Z"}
  },
  "ts": "2020-04-15T13:37:05Z"
}
```

오류 응답:
```json
{
  "action": "get",
  "requestId": "8756",
  "error": {"number": "404", "reason": "unavailable_data", "description": "The requested data was not found."},
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 인가된 Read

트리 노드에 인가가 필요한 경우, 요청에 유효한 JWT 토큰을 값으로 포함하는 "authorization" 필드가 포함되어야 한다.

**예시:**
```json
{
  "action": "get",
  "path": "Vehicle.Powertrain.CombustionEngine.RPM",
  "authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1...Zw_KSsds",
  "requestId": "8657"
}
```

###### 검색 Read

클라이언트는 하나의 요청 메시지로 여러 신호 값을 검색하기 위해 검색 read 요청을 발행할 수 있다. [[CORE]] 사양의 7.1 Paths Filter Operation에 따라 "filter" 객체를 추가하여 실현된다.

**예시:**
```json
{
  "action": "get",
  "path": "Vehicle.Cabin",
  "filter": {"variant": "paths", "parameter": ["Door.*.*.IsOpen", "DriverPosition"]},
  "requestId": "5688"
}
```

응답:
```json
{
  "action": "get",
  "data": [
    {"path": "Vehicle.Cabin.Door.Row1.Left.IsOpen", "dp": {"value": "false", "ts": "2020-04-15T13:37:00Z"}},
    {"path": "Vehicle.Cabin.Door.Row4.Right.IsOpen", "dp": {"value": "true", "ts": "2020-04-15T13:37:01Z"}},
    {"path": "Vehicle.Cabin.DriverPosition", "dp": {"value": "1", "ts": "2020-04-15T07:00:01Z"}}
  ],
  "requestId": "5688",
  "ts": "2020-04-15T07:00:02Z"
}
```

###### 히스토리 Read

클라이언트는 이전에 기록된 데이터 포인트를 검색하기 위해 히스토리 read 요청을 발행할 수 있다.

**예시:**
```json
{
  "action": "get",
  "path": "Vehicle.Acceleration.Longitudinal",
  "filter": {"variant": "history", "parameter": "P2DT12H"},
  "requestId": "5688"
}
```

응답:
```json
{
  "action": "get",
  "data": {
    "path": "Vehicle.Acceleration.Longitudinal",
    "dp": [
      {"value": "0.123", "ts": "2020-04-15T13:00:00Z"},
      {"value": "0.125", "ts": "2020-04-15T13:37:02Z"}
    ]
  },
  "requestId": "5688",
  "ts": "2020-04-15T13:37:02Z"
}
```

###### 신호 탐색 Read

클라이언트는 트리에 대한 메타데이터를 검색하기 위해 신호 탐색 read 요청을 발행할 수 있다.

**예시:**
```json
{
  "action": "get",
  "path": "Vehicle.Powertrain.FuelSystem",
  "filter": {"variant": "metadata", "parameter": "2"},
  "requestId": "5687"
}
```

응답:
```json
{
  "action": "get",
  "requestId": "5687",
  "metadata": {"FuelSystem": {"type": "branch", "children": ["HybridType"]}},
  "ts": "2020-04-15T13:37:00Z"
}
```

##### Update (업데이트)

클라이언트는 신호 값을 설정하기 위해 서버에 요청을 전송할 수 있다.

**setRequest 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| setRequest | action | Action | 예 |
| | path | string | 예 |
| | value | string/array/object | 예 |
| | authorization | string | 선택적 |
| | requestId | string | 예 |
| | ts | string | 선택적 |

**setSuccessResponse 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| setSuccessResponse | action | Action | 예 |
| | requestId | string | 예 |
| | ts | string | 예 |

**예시:**

요청:
```json
{
  "action": "set",
  "path": "Vehicle.Powertrain.Transmission.PerformanceMode",
  "value": "sport",
  "requestId": "5687"
}
```

성공 응답:
```json
{
  "action": "set",
  "requestId": "5687",
  "ts": "2020-04-15T13:37:00Z"
}
```

오류 응답:
```json
{
  "action": "set",
  "requestId": "5687",
  "error": {"number": "404", "reason": "unavailable_data", "description": "The requested data was not found."},
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 인가된 Update

인가가 필요한 경우 요청에 유효한 JWT 토큰을 포함하는 "authorization" 필드가 포함되어야 한다.

**예시:**
```json
{
  "action": "set",
  "path": "Vehicle.Powertrain.Transmission.PerformanceMode",
  "value": "sport",
  "authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1...Zw_KSsds",
  "requestId": "5687"
}
```

##### Subscribe (구독)

클라이언트는 하나 이상의 신호에 대한 구독을 요청하기 위해 `subscribeRequest` 메시지를 전송할 수 있다.

지원되는 구독 변형:
- **timebased**: 이벤트 메시지가 정기적인 시간 간격으로 발행됨
- **change**: 이벤트 메시지가 지정된 대로 값이 변경될 때 발행됨
- **range**: 이벤트 메시지가 값이 지정된 범위 내에 있을 때 발행됨
- **curvelog**: 이벤트 메시지가 버퍼가 가득 찰 때 발행되고 곡선 로깅 알고리즘에 따라 처리됨

**subscribeRequest 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| subscribeRequest | action | Action | 예 |
| | path | string | 예 |
| | filter | string | 예 |
| | authorization | string | 선택적 |
| | dc | string | 선택적 |
| | requestId | string | 예 |

**subscribeSuccessResponse 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| subscribeSuccessResponse | action | Action | 예 |
| | requestId | string | 예 |
| | subscriptionId | string | 예 |
| | ts | string | 예 |

**subscriptionEvent 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| subscriptionEvent | action | Action | 예 |
| | subscriptionId | string | 예 |
| | data | object/array | 예 |
| | ts | string | 예 |

**예시:**

요청:
```json
{
  "action": "subscribe",
  "path": "Vehicle.Powertrain.FuelSystem.Level",
  "filter": {"variant": "timebased", "parameter": {"period": "500"}},
  "requestId": "6578"
}
```

성공 응답:
```json
{
  "action": "subscribe",
  "subscriptionId": "12345",
  "requestId": "6578",
  "ts": "2020-04-15T13:37:00Z"
}
```

오류 응답:
```json
{
  "action": "subscribe",
  "requestId": "6578",
  "error": {"number": "404", "reason": "unavailable_data", "description": "The requested data was not found."},
  "ts": "2020-04-15T13:37:00Z"
}
```

이벤트:
```json
{
  "action": "subscription",
  "subscriptionId": "12345",
  "data": {
    "path": "Vehicle.Powertrain.FuelSystem.Level",
    "dp": {"value": "50", "ts": "2020-04-15T13:37:00Z"}
  },
  "ts": "2020-04-15T13:37:00Z"
}
```

오류 이벤트:
```json
{
  "action": "subscription",
  "subscriptionId": "12345",
  "error": {"number": "401", "reason": "expired_token", "description": "Access token has expired."},
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 인가된 Subscribe

인가가 필요한 경우 요청에 유효한 JWT 토큰을 값으로 포함하는 "authorization" 필드가 포함되어야 한다.

**예시 (범위 구독과 조합):**
```json
{
  "action": "subscribe",
  "path": "Vehicle.Powertrain.FuelSystem.Level",
  "filter": {"variant": "range", "parameter": [{"logic-op": "gt", "boundary": "49"}, {"logic-op": "lt", "boundary": "51"}]},
  "authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1...Zw_KSsds",
  "requestId": "6578"
}
```

###### 곡선 로깅 Subscribe

곡선 로깅 데이터 압축은 구독 요청을 통해 활성화된다. 이벤트 메시지는 버퍼가 가득 찰 때 발행된다.

**예시:**
```json
{
  "action": "subscribe",
  "path": "Vehicle.Powertrain.FuelSystem.Level",
  "filter": {"variant": "curvelog", "parameter": {"maxerr": "0.5", "bufsize": "100"}},
  "requestId": "6578"
}
```

이벤트:
```json
{
  "action": "subscription",
  "subscriptionId": "12345",
  "data": {
    "path": "Vehicle.Powertrain.FuelSystem.Level",
    "dp": [
      {"value": "50", "ts": "2020-04-15T13:38:00Z"},
      {"value": "25", "ts": "2020-04-15T13:39:30Z"}
    ]
  },
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 범위 Subscribe

값의 범위에 대한 구독으로 단일 경계 또는 다중 경계를 가질 수 있다.

**예시 (OR 조합의 다중 경계):**
```json
{
  "action": "subscribe",
  "path": "Vehicle.Powertrain.FuelSystem.Level",
  "filter": {"variant": "range", "parameter": [{"logic-op": "lt", "boundary": "50", "combination-op": "OR"}, {"logic-op": "gt", "boundary": "55"}]},
  "requestId": "6578"
}
```

###### 변경 Subscribe

두 연속 수집 사이에 신호가 변경될 때에 대한 구독.

**예시:**
```json
{
  "action": "subscribe",
  "path": "Vehicle.Powertrain.FuelSystem.Level",
  "filter": {"variant": "change", "parameter": {"logic-op": "gt", "diff": "10"}},
  "requestId": "6578"
}
```

##### Unsubscribe (구독 취소)

구독을 취소하기 위해 클라이언트는 `unsubscribeRequest` 메시지를 전송해야 한다.

**unsubscribeRequest 구조:**

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| unsubscribeRequest | action | Action | 예 |
| | subscriptionId | string | 예 |
| | requestId | string | 예 |

**예시:**

요청:
```json
{
  "action": "unsubscribe",
  "subscriptionId": "12345",
  "requestId": "5786"
}
```

성공 응답:
```json
{
  "action": "unsubscribe",
  "requestId": "5786",
  "ts": "2020-04-15T13:37:00Z"
}
```

오류 응답:
```json
{
  "action": "unsubscribe",
  "requestId": "5786",
  "error": {"number": "400", "reason": "invalid_data", "description": "Data present in the request is invalid."},
  "ts": "2020-04-15T13:37:00Z"
}
```

### 4.2 HTTPS

메시지 데이터 구성 요소는 주로 표준 HTTP 파라미터에 매핑된다. 가장 중요한 편차는 다음과 같다:

- 경로가 URL의 일부로 포함된다.
- 필터 표현식이 쿼리 문자열로 URL에 추가된다.
- "action" 필드 대신 HTTP 메서드 GET과 POST가 사용된다.

subscribe/unsubscribe 작업은 이 전송 프로토콜에서 지원되지 않는다.

#### 4.2.1 세션 수명 관리

##### 초기화

클라이언트는 HTTPS 프로토콜을 사용하여 서버의 지정된 URL에 연결함으로써 보안 세션을 초기화한다. 통신은 포트 443(HTTPS 기본 포트)을 사용해야 한다.

##### 종료

클라이언트 또는 서버 중 하나가 세션 종료를 시작할 수 있다. 세션 종료 시 서버 측 스레드 또는 메모리와 같은 할당된 리소스가 해제된다.

#### 4.2.2 전송 메시지

##### Read (읽기)

클라이언트는 HTTPS GET 요청을 서버에 전송하여 하나 이상의 차량 신호 값을 가져올 수 있다.

**예시:**

요청:
```
GET /Vehicle/Cabin/SeatPosCount   HTTP/1.1
Host: 127.0.0.1:1337
Accept: application/json
```

성공 응답:
```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "data": {
    "path": "Vehicle.Cabin.SeatPosCount",
    "dp": {"value": ["2", "3", "2"], "ts": "2020-04-15T13:37:00Z"}
  },
  "ts": "2020-04-15T13:37:00Z"
}
```

오류 응답:
```
HTTP/1.1 404 Not Found
Content-Type: application/json; charset=utf-8

{
  "error": {"number": "404", "reason": "unavailable_data", "description": "The requested data was not found."},
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 인가된 Read

JWT 토큰은 `Bearer` 접두사 다음에 공백 문자를 붙여 `Authorization` 헤더에 포함되어야 한다.

**예시:**
```
GET /Vehicle/Powertrain/CombustionEngine/RPM   HTTP/1.1
Host: 127.0.0.1:1337
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.xuEv8qrfXu424LZk8bVgr9MQJUIrp1rHcPyZw_KSsds
```

성공 응답:
```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "data": {
    "path": "Vehicle.Powertrain.CombustionEngine.RPM",
    "dp": {"value": "2372", "ts": "2020-04-15T13:37:00Z"}
  },
  "ts": "2020-04-15T13:37:00Z"
}
```

오류 응답:
```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer realm="127.0.0.1:1337",
                         error="invalid_token",
                         error_description="The access token is invalid or expired"
Content-Type: application/json; charset=utf-8

{
  "error": {"number": "401", "reason": "invalid_token", "description": "Access token is invalid."},
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 검색 Read

**예시:**
```
GET /Vehicle/Cabin/Door?filter={"variant":"paths", "parameter":"*/*/IsOpen"}   HTTP/1.1
Host: 127.0.0.1:1337
Accept: application/json
```

응답:
```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "data": [
    {"path": "Vehicle.Cabin.Door.Row1.Left.IsOpen", "dp": {"value": "false", "ts": "2020-04-15T13:37:00Z"}},
    {"path": "Vehicle.Cabin.Door.Row4.Right.IsOpen", "dp": {"value": "true", "ts": "2020-04-15T13:37:00Z"}}
  ],
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 히스토리 Read

**예시:**
```
GET /Vehicle.Acceleration.Longitudinal?filter={"variant":"history", "parameter":"P2DT12H"}   HTTP/1.1
Host: 127.0.0.1:1337
Accept: application/json
```

응답:
```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "data": {
    "path": "Vehicle.Acceleration.Longitudinal",
    "dp": [
      {"value": "0.123", "ts": "2020-04-15T13:00:00Z"},
      {"value": "0.125", "ts": "2020-04-15T13:37:00Z"}
    ]
  },
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 신호 탐색 Read

**예시:**
```
GET /Vehicle/Powertrain/FuelSystem?filter={"variant":"metadata", "parameter":"0"}   HTTP/1.1
Host: 127.0.0.1:1337
Accept: application/json
```

응답:
```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "metadata": {"FuelSystem": {"type": "branch", "description": "Fuel system data.", "children": {"HybridType"}}},
  "ts": "2020-04-15T13:37:00Z"
}
```

##### Update (업데이트)

클라이언트는 신호 값을 업데이트하기 위해 요청을 전송할 수 있다.

**예시:**
```
POST /Vehicle/Powertrain/Transmission/PerformanceMode   HTTP/1.1
Host: 127.0.0.1:1337
Accept: application/json

{
  "value": "sport"
}
```

성공 응답:
```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{
  "ts": "2020-04-15T13:37:00Z"
}
```

###### 인가된 Update

JWT 토큰은 `Bearer` 접두사와 공백 문자를 붙여 `Authorization` 헤더에 포함되어야 한다.

**예시:**
```
POST /Vehicle/Powertrain/Transmission/PerformanceMode   HTTP/1.1
Host: 127.0.0.1:1337
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.xuEv8qrfXu424LZk8bVgr9MQJUIrp1rHcPyZw_KSsds

{
  "value": "sport"
}
```

### 4.3 MQTT

MQTT 프로토콜은 게시-구독 통신 모델로 작동하지만, VISS는 클라이언트-서버 상호작용 모델을 기반으로 설계되었다. 이 차이를 해결하기 위해 MQTT 위에 경량 애플리케이션 수준 프로토콜이 도입된다. 이 프로토콜은 VISS 메시지를 MQTT 메시지 내에 캡슐화하고 클라이언트-서버 시맨틱을 지원하는 데 필요한 추가 메타데이터를 포함한다.

MQTT를 통해 전송되는 VISS 메시지는 수정 없이 기본 페이로드 형식을 준수해야 한다.

#### 4.3.1 애플리케이션 수준 프로토콜

[[CORE]] 사양의 5절 Interface에서 설명된 전체 VISSv3 인터페이스를 MQTT가 지원하려면 MQTT 위에서 실행되는 애플리케이션 수준 프로토콜이 필요하다.

클라이언트-서버 패턴을 에뮬레이션하기 위해 차량 서버는 차량 측 클라이언트를 통해 `VID/Vehicle` 이름의 토픽으로 브로커에 구독해야 한다. 여기서 VID는 차량을 접근 제어 에코시스템에 연결하는 고유 식별자다.

클라우드 측 클라이언트가 차량 서버에 요청을 보내려는 경우:

1. 고유한 토픽 이름을 생성하고 브로커를 통해 해당 토픽을 구독한다.
2. 다음 구조의 JSON 형식 페이로드를 생성한다:
   ```json
   {"topic": "aUniqueTopic", "request": "VISSv3Request"}
   ```
3. 이 JSON 메시지를 `VID/Vehicle` 토픽으로 브로커에 게시한다.
4. 브로커는 이 메시지를 차량 클라이언트로 전달하고, 차량 클라이언트는 "request" 필드의 값을 추출하여 차량 서버로 전달한다.
5. 차량 서버로부터 응답을 받으면 차량 클라이언트는 수신된 메시지의 "topic" 필드에 지정된 토픽으로 응답 메시지를 게시한다.

구독 요청의 경우 차량 클라이언트는 구독 응답의 subscriptionId를 구독 요청과 연관된 토픽 이름과 함께 저장해야 한다.

![Message flow of VISSv3 over MQTT](images/mqtt_vissv3_protocol.jpg)

#### 4.3.2 보안 측면

MQTT 아키텍처는 클라이언트와 서버 엔드포인트 사이에 브로커를 중간자로 필요로 한다. 각 TLS 채널이 브로커에서 종료되므로 브로커는 이 엔드포인트 간의 모든 평문 통신에 완전히 접근할 수 있다. 이 보안 고려사항은 MQTT를 전송 프로토콜로 선택할 때 고려해야 한다.

#### 4.3.3 전송 메시지

"애플리케이션 수준 프로토콜" 절에서 설명했듯이 브로커로 전송되는 각 요청 메시지에는 두 개의 JSON 형식 키-값 쌍이 포함되어야 한다. request 키의 값은 차량 서버를 대상으로 하는 클라이언트 요청을 포함하는 문자열이어야 하며, 이 요청은 이 사양에서 정의된 기본 페이로드 형식을 엄격히 준수해야 한다.

### 4.4 gRPC

gRPC 프로토콜은 메시지 직렬화에 Protocol Buffers(Protobuf)를 사용한다. Protobuf 메시지 정의는 .proto 파일로 지정되어야 한다. VISS 기본 페이로드 형식의 인코딩을 정의하는 .proto 파일은 [[PAYLOAD ENCODING]] 사양에 제공된다.

### 4.5 Unix Domain Sockets (UDS)

UDS 프로토콜은 편차 없이 기본 페이로드 형식으로 사용되어야 한다. 서버가 채널을 수신하는 데 사용하는 소켓 파일 이름은 "udsMgr.sock"이어야 한다. 이 파일은 파일 시스템에서 슈퍼유저가 아닌 클라이언트가 읽고 쓸 수 있는 디렉터리에 위치해야 한다. Linux 시스템에서 예시 디렉터리는 "/var/tmp/vissv2/"다.

---

## 5. 정의

### 5.1 용어 정의

| 속성 | 유형 | 설명 |
|------|------|------|
| action | Action | 클라이언트가 요청하거나 서버가 전달하는 액션 유형 |
| path | string | [HIM](https://github.com/COVESA/hierarchical_information_model)에 정의된 트리 내 노드 경로 |
| requestId | string | 클라이언트가 할당하는 고유 ID. 서버가 응답에서 반환하며 클라이언트가 요청과 응답을 연결하는 데 사용. 정수 또는 UUID일 수 있음 |
| subscriptionId | string | 각 구독 세션을 고유하게 식별하기 위해 서버가 반환하는 값 |
| authorization | string | JWT(JSON Web Token) 형식 보안 토큰 |
| data | object/array | 경로와 dp(데이터 포인트)로 구성된 하나 이상의 객체를 포함하는 데이터 |
| dp | object/array | 값과 ts(타임스탬프)로 구성된 하나 이상의 객체를 포함하는 데이터 포인트 |
| ts | string | 값 수집을 나타내는 UTC(협정 세계시) 타임스탬프 |
| value | string/object/array | 신호 경로와 연관된 실제 값 |
| filter | string | 구독을 최적화하기 위한 필터를 정의하는 쿼리 문자열 |
| dc | string | 경로 및/또는 타임스탬프 데이터에 적용된 체계를 식별하는 데이터 압축 문자열 |
| metadata | object | 잠재적으로 사용 가능한 신호 (서브)트리를 설명하는 메타데이터 |
| error | Error | 오류 코드, 이유, 설명을 포함하는 객체 |

### 5.2 Action 정의

Action 열거형은 클라이언트가 요청하는 액션 유형을 정의하는 데 사용된다. 모든 클라이언트 메시지는 action 이름/값 쌍을 포함하는 JSON 구조를 포함해야 하며, action 속성의 값은 열거형에서 지정한 값 중 하나여야 한다:

- **get**: 클라이언트가 하나 이상의 값을 읽을 수 있게 한다.
- **set**: 클라이언트가 하나의 값을 업데이트할 수 있게 한다.
- **subscribe**: 클라이언트가 하나 이상의 차량 신호 값을 포함하는 JSON 데이터 구조를 포함하는 이벤트 메시지를 요청할 수 있게 한다.
- **unsubscribe**: 클라이언트가 해당 구독을 기반으로 더 이상 이벤트 메시지를 받지 않도록 요청할 수 있게 한다.
- **subscription**: 서버가 하나 이상의 차량 신호 값을 포함하는 JSON 데이터 구조를 포함하는 이벤트 메시지를 클라이언트에게 전송할 수 있게 한다.

### 5.3 Error 정의

오류 번호는 RFC2616에 정의된 상태 코드여야 한다. 오류 이유는 짧아야 하며, 두세 단어를 밑줄로 연결한 형태여야 한다.

| 객체 이름 | 속성 | 유형 | 필수 여부 |
|---------|------|------|---------|
| Error | number | string | 예 |
| | reason | string | 예 |
| | description | string | 예 |
