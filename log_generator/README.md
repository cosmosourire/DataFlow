이벤트 메타데이터
* event_id (string): 이벤트 고유 ID. 중복 수집 방지(멱등성 체크)와 재처리에 사용.
* schema_version (number): 페이로드 스키마 버전. 파이프라인/파서가 호환성 판단에 씀.
* event_time (RFC3339 string): 이벤트가 발생한 시각(UTC). 사용자 행동 기준의 타임스탬프.
* ingest_time (RFC3339 string): 이벤트가 수집/적재된 시각(UTC). 수집 지연 측정에 사용.
* service (string): 이벤트를 발생시킨 서비스/컴포넌트 이름(예: web-frontend).
분산 추적(Tracing)
* trace_id (string): 하나의 요청 흐름(트랜잭션)을 관통하는 추적 ID. 여러 서비스 로그를 묶는 키.
* span_id (string): 트레이스 내 개별 작업 단위(스팬)의 ID. 특정 구간 성능 분석에 활용.
사용자 & 세션
* user_id (string): 로그인한 사용자 식별자. 비로그인 시 비울 수 있음. PII와 분리된 내부 ID 권장.
* anonymous_id (string): 비로그인/초기 세션 식별자(쿠키/디바이스 기반). 로그인 전 퍼널 분석에 중요.
* user_logged_in (bool): 로그인 상태 플래그. user_id 유무와 교차 검증.
* session_id (string): 세션 식별자. 방문 단위 묶음(세션 길이, 이탈률, 전환경로 계산).
행동(이벤트 본문)
* action (string): 수행한 행동 유형(예: click, view_item, purchase). 분석 기준 축.
* page (string): 발생 위치(URL 경로/화면 라우트). 페이지/스크린별 성과 분석.
* product_id (string): 해당 행동이 연관된 상품 ID. 상품 상세/전환 추적.
* device (string): 클라이언트 구분(예: ios, android, web). 플랫폼별 분석.
* os (string), os_version (string): 운영체제와 버전. 호환성 이슈/크래시 상관 분석.
* app_version (string): 앱/프론트엔드 버전. 배포(릴리즈) 효과와 버그 롤백 판단.
* user_agent (string): 브라우저/앱 UA 문자열. 세부 디바이스/엔진 파싱 가능.
* locale (string): 사용자 언어/지역 설정(예: ko-KR). 현지화 성과 분석.
* timezone (string): 사용자의 타임존(예: Asia/Seoul). 로컬 시간대 리포팅에 필요.
* region (string): 지역/국가 코드(예: KR). 국가별 마케팅·규제 대응.
* network_type (string): 연결 유형(wifi, cellular 등). 네트워크 품질과 성능 상관 분석.
성능/결과
* latency_ms (number): 요청 왕복 지연(ms). SLO/성능 모니터링 핵심 지표.
* status_code (number): 서버 응답 코드(200/4xx/5xx). 실패 원인 분류.
* success (bool): 성공 여부. 보통 status_code와 일치시킴(200대=성공).
비즈니스 값(선택)
* value (number): 금액/가치(예: 결제 금액, 장바구니 가치). 행동 가치 모델링에 사용.
* currency (string): 통화 코드(예: KRW). 다국 통화 리포팅 정규화.
유입/마케팅(Attribution)
* referrer (string): 직전 페이지/검색 쿼리. 온사이트 내 이동/검색 성과 분석.
* utm_source (string): 트래픽 출처(예: naver).
* utm_medium (string): 매체 유형(예: cpc, email).
* utm_campaign (string): 캠페인 이름(예: fall_sale).

```json
{
  "event_id": "0e5f2d1c-1d3a-4b8d-9c3a-0f6a8e25b0ab",
  "schema_version": 2,
  "event_time": "2025-09-25T11:03:21.123Z",
  "ingest_time": "2025-09-25T11:03:21.800Z",
  "service": "web-frontend",
  "trace_id": "8e3f...b21",
  "span_id": "a19c...02f",

  "user_id": "u_94321",
  "anonymous_id": "anon_b7f3...",
  "user_logged_in": true,
  "session_id": "s_1882",

  "action": "click",
  "page": "/product/42",
  "product_id": "42",
  "device": "ios",
  "os": "iOS",
  "os_version": "17.5",
  "app_version": "5.3.1",
  "user_agent": "Mozilla/5.0 ...",
  "locale": "ko-KR",
  "timezone": "Asia/Seoul",
  "region": "KR",
  "network_type": "wifi",

  "latency_ms": 120,
  "status_code": 200,
  "success": true,
  "value": 0.0,
  "currency": "KRW",

  "referrer": "/search?q=abc",
  "utm_source": "naver",
  "utm_medium": "cpc",
  "utm_campaign": "fall_sale"
}
```


