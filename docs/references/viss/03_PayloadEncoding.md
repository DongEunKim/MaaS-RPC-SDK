> 원문: [VISSv3.1_PayloadEncoding.html](VISSv3.1_PayloadEncoding.html) | 출처: COVESA Vehicle Information Service Specification v3.1

# COVESA VISS 버전 3.1 — Payload Encoding

**편집자:** Ulf Bjorkengren (Ford Motor Company), 이원석(Wonsuk Lee) (한국전자통신연구원 ETRI)

Copyright © 2026 COVESA®.

---

## 초록

Vehicle Information Service Specification(VISS)은 차량 네트워크 내 제어 장치 센서로부터 수집된 신호를 포함한 차량 정보에 접근하기 위한 서비스다. 이 정보는 COVESA [Hierarchical Information Model](https://github.com/COVESA/hierarchical_information_model)(HIM)에서 정의하는 계층적 트리 형태의 분류 체계로 노출되며, JSON 형식으로 제공된다. VISS 서비스는 차량 내부 또는 이미 오프보딩된 데이터를 사용하는 외부 서버에 호스팅될 수 있다.

이 사양은 세 부분으로 구성된다: [[CORE]], [[TRANSPORT]], PAYLOAD ENCODING. 이 문서인 VISS 버전 3.1 PAYLOAD ENCODING 사양은 일부 경우에 사용되는 VISSv3.1 페이로드 인코딩을 설명한다. 함께 제공되는 [[CORE]] 사양은 메시징 계층을 설명하고, [[TRANSPORT]] 사양은 일부 전송 프로토콜에서 사용되는 CORE 사양의 편차를 설명한다.

---

## 1. 소개

이 사양은 일부 전송 프로토콜에서 사용되는 페이로드 인코딩을 설명한다.

---

## 2. 용어

'VISSv3.1'이라는 약어는 이 문서, VISS 버전 3.1 사양을 가리키는 데 사용된다. 'HIM'이라는 약어는 COVESA에서 호스팅하는 ['Hierarchical Information Model version 1.0 Vehicle data profile'](https://github.com/COVESA/hierarchical_information_model)을 가리킨다. 'VSS'라는 약어는 COVESA에서 호스팅하는 ['Vehicle Signal Specification'](https://github.com/COVESA/vehicle_signal_specification)을 가리킨다. 'WebSocket'은 [W3C WebSocket API](https://www.w3.org/TR/websockets/) 및 RFC6455에 정의된 대로 사용된다.

---

## 3. 전송 페이로드

기본 페이로드 형식은 JSON이다. 즉, 구현이 의도적으로 이를 벗어나지 않는 한 클라이언트가 수신하는 페이로드는 JSON 형식이어야 한다. 다양한 전송 프로토콜에 대한 메시지 JSON 페이로드 형식은 [[CORE]] 사양을 참조한다.

---

## 4. Protobuf 인코딩

Protocol Buffers(Protobuf)는 Google이 개발한 언어 중립적, 플랫폼 중립적, 확장 가능한 구조화된 데이터 직렬화 메커니즘이다. 이 인코딩 방법은 효율적인 데이터 직렬화 및 역직렬화에 특히 유용하며, 차량 정보 서비스에 중요한 낮은 지연 시간과 축소된 페이로드 크기를 보장한다.

VISSv3.1에 Protobuf 인코딩을 통합함으로써 자동차 에코시스템 내 다양한 시스템 및 플랫폼에서 원활한 구현과 상호운용성이 가능해진다.

### 4.1 Protobuf 스키마 정의

다음 Protobuf 스키마는 VISS 메시지 페이로드를 정의한다. 파일(예: "viss.proto")에 저장하면 Protobuf 컴파일러(protoc)를 사용하여 JSON과 Protobuf 형식 간의 인코딩 및 디코딩 구현에 호출할 수 있는 헬퍼 함수를 생성할 수 있다. 아래 Protobuf 스키마에는 protoc 컴파일러가 gRPC 기반 통신 프레임워크를 생성하는 데 사용하는 "service" 절도 포함되어 있다.

```protobuf
syntax = "proto3";
package grpcProtobufMessages;

enum ResponseStatus {
    SUCCESS = 0;
    ERROR = 1;
}

enum SubscribeResponseType {
    RESPONSE = 0;
    EVENT = 1;
}

service VISS {
  rpc GetRequest (GetRequestMessage) returns (GetResponseMessage);

  rpc SetRequest (SetRequestMessage) returns (SetResponseMessage);

  rpc SubscribeRequest (SubscribeRequestMessage) returns (stream SubscribeStreamMessage);

  rpc UnsubscribeRequest (UnsubscribeRequestMessage) returns (UnsubscribeResponseMessage);
}

message ErrorResponseMessage {
    string Number = 1;
    string Reason = 2;
    string Description = 3;
}

message FilterExpressions {
  message FilterExpression {
    enum FilterVariant {
        PATHS = 0;
        TIMEBASED = 1;
        RANGE = 2;
        CHANGE = 3;
        CURVELOG = 4;
        HISTORY = 5;
        METADATA = 6;
    }
    FilterVariant Variant = 1;

    message FilterValue {
        message PathsValue {
            repeated string RelativePath = 1;
        }
        optional PathsValue ValuePaths = 1;

        message TimebasedValue {
            string Period = 1;
        }
        optional TimebasedValue ValueTimebased = 2;

        message RangeValue {
            string LogicOperator = 1;
            string Boundary = 2;
        }
        repeated RangeValue ValueRange = 3;

        message ChangeValue {
            string LogicOperator = 1;
            string Diff = 2;
        }
        optional ChangeValue ValueChange = 4;

        message CurvelogValue {
            string MaxErr = 1;
            string BufSize = 2;
        }
        optional CurvelogValue ValueCurvelog = 5;

        message HistoryValue {
            string TimePeriod = 1;  //ISO8601 period expression
        }
        optional HistoryValue ValueHistory = 6;

        message MetadataValue {
            string Tree = 1;
        }
        optional MetadataValue ValueMetadata = 7;
    }
    FilterValue Value = 2;
  }
  repeated FilterExpression FilterExp = 1;
}

message DataPackages {
    message DataPackage {
        string Path = 1;

        message DataPoint {
            string Value = 1;
            string Ts = 2;
        }
        repeated DataPoint Dp = 2;
    }
    repeated DataPackage Data = 1;
}

message GetRequestMessage {
        string Path = 1;
        optional FilterExpressions Filter = 2;
        optional string Authorization = 3;
        optional string DC = 4;
        string RequestId = 5;
}

message GetResponseMessage {
        ResponseStatus Status = 1;
        message SuccessResponseMessage {
            optional DataPackages DataPack = 1;
            optional string Metadata = 2; // replaces DataPack in metadata variant
        }
        optional SuccessResponseMessage SuccessResponse = 2;
        optional ErrorResponseMessage ErrorResponse = 3;
        string RequestId = 4;
        string Ts = 5;
        optional string Authorization = 6;
}

message SetRequestMessage {
        string Path = 1;
        string Value = 2;
        optional string Authorization = 3;
        string RequestId = 4;
        optional string Ts = 5;
}

message SetResponseMessage {
        ResponseStatus Status = 1;
        optional ErrorResponseMessage ErrorResponse = 2;
        string RequestId = 3;
        string Ts = 4;
        optional string Authorization = 5;
}

message SubscribeRequestMessage {
        string Path = 1;
        optional FilterExpressions Filter = 2;
        optional string Authorization = 3;
        optional string DC = 4;
        string RequestId = 5;
}

message SubscribeStreamMessage {
    SubscribeResponseType MType = 1;
    ResponseStatus Status = 2;

    message SubscribeResponseMessage {
        optional ErrorResponseMessage ErrorResponse = 1;
        optional string SubscriptionId = 2;
        string RequestId = 3;
        string Ts = 4;
        optional string Authorization = 5;
    }
    optional SubscribeResponseMessage Response = 3;

    message SubscribeEventMessage {
        string SubscriptionId = 1;
        message SuccessResponseMessage {
            DataPackages DataPack = 1;
        }
        optional SuccessResponseMessage SuccessResponse = 2;
        optional ErrorResponseMessage ErrorResponse = 3;
        string Ts = 4;
    }
    optional SubscribeEventMessage Event = 4;
}

message UnsubscribeRequestMessage {
        string SubscriptionId = 1;
        string RequestId = 2;
}

message UnsubscribeResponseMessage {
        ResponseStatus Status = 1;
        optional ErrorResponseMessage ErrorResponse = 2;
        string RequestId = 3;
        string Ts = 4;
}
```

*VISS 버전 3.1을 위한 Protobuf 스키마 파일*

### 4.2 전송 프로토콜 실현

이 절에서는 gRPC, WebSocket, MQTT를 포함한 다양한 전송 프로토콜에서 Protobuf 인코딩이 어떻게 실현되는지 설명한다.

#### 4.2.1 gRPC 실현

Protobuf 스키마의 'service' 절은 Protobuf 컴파일러(protoc)가 gRPC 기반 통신을 위한 코드를 자동으로 생성하는 데 사용된다. 이 생성된 코드는 네트워크 통신 계층을 추상화하여 개발자가 애플리케이션 로직에 집중할 수 있게 함으로써 서버와 클라이언트 상호작용 개발을 단순화한다.

#### 4.2.2 WebSocket 실현

서버가 WebSocket 프로토콜을 통한 Protobuf 페이로드 인코딩을 지원하는 경우, 클라이언트가 이를 어떻게 구성하는지를 서버 기능 데이터에 표시해야 한다. 두 가지 메커니즘이 정의된다:

- **별도 포트 번호**: 권장 포트 번호는 6444다.
- **서브 프로토콜 지정**: 서버 연결 시 서브 프로토콜이 지정되어야 한다. 권장 서브 프로토콜 이름은 "VISS-protoenc"다.

#### 4.2.3 MQTT 실현

서버가 MQTT 프로토콜을 통한 Protobuf 페이로드 인코딩을 지원하는 경우, 서버 기능 데이터에 서버가 Protobuf 인코딩 페이로드를 포함한 요청을 발행하려는 클라이언트를 수신 대기하는 데 사용하는 토픽 이름을 표시해야 한다. 권장 토픽 이름은 인코딩되지 않은 페이로드 채널에 사용되는 토픽 이름 뒤에 "/protobuf" 문자열을 추가하는 것이다.

### 4.3 Protobuf를 사용한 인코딩 및 디코딩

Protobuf 컴파일러는 Protobuf 스키마를 사용하여 다양한 VISS 메시지에 대해 JSON과 Protobuf 형식 간의 인코딩 및 디코딩을 구현하는 데 호출할 수 있는 헬퍼 함수의 API를 생성한다. 인코딩과 디코딩의 구현은 이 사양의 범위를 벗어난다.

---

## 5. JSON 스키마 기반 인코딩

OpenAPI 및 AsyncAPI와 같은 여러 IDL(Interface Definition Language)은 JSON 스키마 표현을 입력으로 활용할 수 있다. [[CORE]] 사양에 있는 VISSv3.1 인터페이스의 JSON 스키마 표현은 그러한 목적에 사용될 수 있다.
