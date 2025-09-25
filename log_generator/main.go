// go run main.go -n 100 -rate 20 -pretty
// go run main.go -duration 5s -rate 50            # 5초 동안 초당 50개
// go run main.go -n 1 -pretty                      # 한 건만 보기 좋게 출력

package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	EventID    string `json:"event_id"`
	SchemaVer  int    `json:"schema_version"`
	EventTime  string `json:"event_time"`
	IngestTime string `json:"ingest_time"`
	Service    string `json:"service"`
	TraceID    string `json:"trace_id"`
	SpanID     string `json:"span_id"`

	UserID       string `json:"user_id"`
	AnonymousID  string `json:"anonymous_id"`
	UserLoggedIn bool   `json:"user_logged_in"`
	SessionID    string `json:"session_id"`

	Action      string `json:"action"`
	Page        string `json:"page"`
	ProductID   string `json:"product_id"`
	Device      string `json:"device"`
	OS          string `json:"os"`
	OSVersion   string `json:"os_version"`
	AppVersion  string `json:"app_version"`
	UserAgent   string `json:"user_agent"`
	Locale      string `json:"locale"`
	Timezone    string `json:"timezone"`
	Region      string `json:"region"`
	NetworkType string `json:"network_type"`

	LatencyMs  int     `json:"latency_ms"`
	StatusCode int     `json:"status_code"`
	Success    bool    `json:"success"`
	Value      float64 `json:"value"`
	Currency   string  `json:"currency"`

	Referrer    string `json:"referrer"`
	UTMSource   string `json:"utm_source"`
	UTMMedium   string `json:"utm_medium"`
	UTMCampaign string `json:"utm_campaign"`
}

func main() {
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"), // 브로커 주소
		Topic:        "events",                    // 보낼 토픽
		Balancer:     &kafka.LeastBytes{},         // 파티션 선택 전략
		BatchTimeout: 50 * time.Millisecond,       // 묶음 전송 지연(튜닝 포인트)
		// RequiredAcks: 1, // 기본값(리더 ack). 더 강하게 하려면 kafka.RequireAll 사용
	}
	defer w.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		ev := randomEvent()

		// JSON 직렬화
		b, err := json.Marshal(ev)
		if err != nil {
			log.Fatalf("json marshal: %v", err)
		}

		// Kafka로 전송
		err = w.WriteMessages(ctx, kafka.Message{
			// Key:   []byte("optional-key"), // 키를 쓰면 파티션 고정
			Value: b,
		})
		if err != nil {
			log.Fatalf("write message: %v", err)
		}
	}
}

func randomEvent() Event {
	now := time.Now().UTC()
	// 이벤트 시간은 최근 2분 안팎으로 분포
	eventTime := now.Add(jitter(-90, 30))       // 과거 90초 ~ 미래 30초
	ingestTime := eventTime.Add(jitter(5, 500)) // 처리 지연 5~500ms

	service := pick([]string{"web-frontend", "checkout", "catalog", "auth"}, nil)

	action := pick([]string{"pageview", "click", "view_item", "add_to_cart", "purchase"}, []int{40, 30, 15, 10, 5})
	page := randomPage(action)
	productID := pickProductID(page)

	device := pick([]string{"ios", "android", "web"}, []int{40, 40, 20})
	osName, osVer, ua, appVer := deviceProfile(device)

	latency := clippedNormalInt(120, 60, 5, 2000) // 평균 120ms, 최소 5, 최대 2000
	status, ok := randomStatus()
	success := ok

	// 구매일 경우 금액 분포
	val := 0.0
	curr := "KRW"
	if action == "purchase" {
		val = toKRW(randNorm(35000, 20000, 1000, 500000)) // 1천원~50만원
	}

	ref := randomReferrer()
	utm := randomUTM()

	return Event{
		EventID:    uuid4(),
		SchemaVer:  2,
		EventTime:  eventTime.Format(time.RFC3339Nano),
		IngestTime: ingestTime.Format(time.RFC3339Nano),
		Service:    service,
		TraceID:    hexString(16),
		SpanID:     hexString(8),

		UserID:       maybe(75, randomUserID(), ""), // 75% 로그인
		AnonymousID:  "anon_" + hexString(6),
		UserLoggedIn: rand.Intn(100) < 75,
		SessionID:    "s_" + randomNumString(4),

		Action:      action,
		Page:        page,
		ProductID:   productID,
		Device:      device,
		OS:          osName,
		OSVersion:   osVer,
		AppVersion:  appVer,
		UserAgent:   ua,
		Locale:      "ko-KR",
		Timezone:    "Asia/Seoul",
		Region:      pick([]string{"KR", "US", "JP"}, []int{90, 7, 3}),
		NetworkType: pick([]string{"wifi", "cellular", "ethernet"}, []int{80, 19, 1}),

		LatencyMs:  latency,
		StatusCode: status,
		Success:    success,
		Value:      val,
		Currency:   curr,

		Referrer:    ref,
		UTMSource:   utm[0],
		UTMMedium:   utm[1],
		UTMCampaign: utm[2],
	}
}

func randomPage(action string) string {
	paths := []string{
		"/", "/search?q=abc", "/search?q=shoes", "/category/men", "/category/women",
		"/product/42", "/product/77", "/cart", "/checkout",
	}
	// 행동별로 페이지 가중치
	switch action {
	case "purchase", "add_to_cart":
		return pick([]string{"/product/42", "/product/77", "/cart", "/checkout"}, []int{40, 30, 20, 10})
	case "view_item":
		return pick([]string{"/product/42", "/product/77"}, []int{60, 40})
	default:
		return pick(paths, nil)
	}
}

func pickProductID(page string) string {
	if strings.HasPrefix(page, "/product/") {
		return strings.TrimPrefix(page, "/product/")
	}
	ids := []string{"", "42", "77", "13", "108"}
	return pick(ids, []int{50, 20, 15, 10, 5}) // 절반은 미지정
}

func deviceProfile(device string) (osName, osVer, ua, appVer string) {
	switch device {
	case "ios":
		osName = "iOS"
		osVer = pick([]string{"16.7", "17.0", "17.4", "17.5", "18.0"}, []int{10, 20, 25, 30, 15})
		appVer = pick([]string{"5.2.0", "5.3.1", "5.4.0"}, []int{20, 60, 20})
		ua = "Mozilla/5.0 (iPhone; CPU iPhone OS " + osVer + " like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
	case "android":
		osName = "Android"
		osVer = pick([]string{"12", "13", "14"}, []int{20, 45, 35})
		appVer = pick([]string{"5.2.0", "5.3.1", "5.4.0"}, []int{20, 60, 20})
		ua = "Mozilla/5.0 (Linux; Android " + osVer + ") AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
	default:
		osName = "macOS"
		osVer = pick([]string{"12.7", "13.6", "14.5"}, []int{20, 40, 40})
		appVer = "web"
		ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X " + osVer + ") AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	}
	return
}

func randomStatus() (status int, ok bool) {
	// 정상 92%, 클라에러 4%, 서버에러 4%
	r := rand.Intn(100)
	switch {
	case r < 92:
		return 200, true
	case r < 96:
		return pickInt([]int{400, 401, 403, 404}, nil), false
	default:
		return pickInt([]int{500, 502, 503, 504}, nil), false
	}
}

func randomReferrer() string {
	return pick(
		[]string{"/", "/search?q=abc", "/search?q=best+deal", "/category/men", "/category/women", ""},
		[]int{10, 30, 20, 15, 15, 10},
	)
}

func randomUTM() [3]string {
	// 70%는 캠페인 태깅, 30%는 공백
	if rand.Intn(100) < 70 {
		src := pick([]string{"naver", "google", "kakao", "facebook", "newsletter"}, nil)
		med := pick([]string{"cpc", "organic", "email", "social"}, []int{50, 20, 15, 15})
		cmp := pick([]string{"fall_sale", "brand_kw", "retargeting", "weekly_digest"}, nil)
		return [3]string{src, med, cmp}
	}
	return [3]string{"", "", ""}
}

func randomUserID() string {
	return fmt.Sprintf("u_%d", 90000+rand.Intn(5000))
}

func uuid4() string {
	// 간단한 v4 UUID 생성 (표준 포맷)
	b := make([]byte, 16)
	crand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func hexString(nBytes int) string {
	b := make([]byte, nBytes)
	crand.Read(b)
	return hex.EncodeToString(b)
}

func randomNumString(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteByte(byte('0' + rand.Intn(10)))
	}
	return sb.String()
}

func jitter(minMs, maxMs int) time.Duration {
	if maxMs < minMs {
		minMs, maxMs = maxMs, minMs
	}
	d := minMs + rand.Intn(maxMs-minMs+1)
	return time.Duration(d) * time.Millisecond
}

func clippedNormalInt(mean, stddev, min, max int) int {
	v := int(randNorm(float64(mean), float64(stddev), float64(min), float64(max)))
	return v
}

func randNorm(mean, stddev, min, max float64) float64 {
	// 정규분포 샘플 후 클리핑
	v := rand.NormFloat64()*stddev + mean
	if v < min {
		v = min
	}
	if v > max {
		v = max
	}
	return v
}

func toKRW(v float64) float64 {
	// KRW는 소수점 거의 없지만 원본 스키마가 float이므로 반올림 처리
	return float64(int(v/100.0+0.5)) * 100.0
}

func pick[T any](vals []T, weights []int) T {
	if len(vals) == 0 {
		panic("pick: empty slice")
	}
	if len(weights) == 0 {
		return vals[rand.Intn(len(vals))]
	}
	sum := 0
	for _, w := range weights {
		sum += w
	}
	r := rand.Intn(sum)
	acc := 0
	for i, w := range weights {
		acc += w
		if r < acc {
			return vals[i]
		}
	}
	return vals[len(vals)-1]
}

func pickInt(vals []int, weights []int) int {
	return pick(vals, weights)
}

func maybe[T any](pct int, a, b T) T {
	if rand.Intn(100) < pct {
		return a
	}
	return b
}
