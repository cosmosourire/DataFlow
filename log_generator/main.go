// 예시 실행법
// go run main.go -users 1000 -duration 5m -pretty
// go run main.go -duration 5s -rate 50              # 5초 동안 초당 50개
// go run main.go -n 1 -pretty                       # 한 건만 보기 좋게 출력

package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

/*
이 프로그램이 하는 일(한 줄 요약)
---------------------------------
"매 초"마다 이번 초에 만들 이벤트 개수를 대략 정하고(자연스러운 랜덤),
그 개수를 '헤비 유저가 더 자주 뽑히도록' 사용자에게 배분해서
카프카 토픽으로 전송합니다.

핵심 포인트
- 총량 결정: users ×(1인당 분당 평균)×(시간대 보정)×(스파이크) → 초당 기대값
- 자연스러운 들쭉날쭉: 포아송 샘플(수학 몰라도 '자연 랜덤'으로 생각)
- 헤비 유저: 사용자마다 '가중치'를 줘서 무거운 유저에게 이벤트가 더 자주 배정
- 카프카 전송: segmentio/kafka-go 사용, 묶음으로 write
*/

// 실제로 전송되는 이벤트 JSON 스키마
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

/* ============================ 실행 옵션(Flags) ============================

사용 예)
- 사용자 기반: go run main.go -users 1000 -duration 5m -pretty
- 고정 속도:  go run main.go -duration 10s -rate 80
- N건만 생성: go run main.go -n 1000

--------------------------------------------------------------------------- */

var (
	// 총량/타이밍 관련
	users             = flag.Int("users", 1000, "활성 사용자 수")
	basePerUserPerMin = flag.Float64("base_per_user_per_min", 0.05, "1인당 '분당' 평균 이벤트 수 (rate 미지정 시 사용)")
	ratePerSec        = flag.Float64("rate", 0, "초당 고정 생성량(>0이면 users 기반 계산 무시)")
	duration          = flag.Duration("duration", 5*time.Minute, "시뮬레이션 수행 시간(-n>0이면 무시)")
	nTotal            = flag.Int("n", 0, "정확히 N건만 생성하고 종료(-duration 무시)")
	pretty            = flag.Bool("pretty", false, "표준출력에 예쁘게 JSON 출력(카프카 전송은 계속 수행)")

	// 헤비 유저 가중치 분포
	dist      = flag.String("dist", "lognorm", "유저 가중치 분포: lognorm | pareto")
	sigma     = flag.Float64("sigma", 1.0, "로그정규 시그마(클수록 상위 꼬리 두꺼움)")
	mu        = flag.Float64("mu", -0.5, "로그정규 뮤(로그-평균)")
	alpha     = flag.Float64("alpha", 1.5, "파레토 알파(작을수록 꼬리 두꺼움)")
	paretoMin = flag.Float64("x_min", 1.0, "파레토 최소값")

	// 카프카 설정
	brokers = flag.String("brokers", "localhost:32000", "카프카 브로커들(콤마 구분)")
	topic   = flag.String("topic", "events.data", "카프카 토픽")

	// 소량의 랜덤 흔들림(자연스러운 출렁임 용)
	jitterRatio = flag.Float64("jitter_ratio", 0.10, "초당 기대값에 곱하는 ±비율 랜덤")
)

/* ======================= 시간대/스파이크 보정값 =======================

- hourBoost: 시간대에 따른 평균 레벨 차이(저녁↑, 새벽↓)
- spikeBoost: 캠페인/라이브 시작 등 특정 시간 구간에 배수로 튀게

둘 다 '곱하기'로 반영됩니다.
----------------------------------------------------------------------- */

var hourBoost = map[int]float64{
	0: 0.3, 1: 0.3, 2: 0.3, 3: 0.3, 4: 0.3, 5: 0.3, 6: 0.3,
	7: 0.8, 8: 0.8, 9: 0.8, 10: 0.8, 11: 0.8,
	12: 1.2, 13: 1.2,
	14: 1.0, 15: 1.0, 16: 1.0, 17: 1.0,
	18: 1.5, 19: 1.5, 20: 1.5, 21: 1.5, 22: 1.5,
	23: 0.6,
}

// 수요일 20~21시에는 ×3으로 튀게(원하면 시간대/조건 변경)
func spikeBoost(d time.Weekday, h int) float64 {
	if d == time.Wednesday && (h == 20 || h == 21) {
		return 3.0
	}
	return 1.0
}

/* ============================= 유틸 함수들 ============================= */

// 기대값 x에 대해 ±ratio 범위로 곱셈형 흔들림(예: ratio=0.1 → 0.9~1.1 배)
func jitterMul(x, ratio float64) float64 {
	min := 1.0 - ratio
	max := 1.0 + ratio
	return x * (min + rand.Float64()*(max-min))
}

// 포아송 샘플러: 평균 lambda일 때 "자연스러운 들쭉날쭉" 개수를 반환
// (lambda가 크면 정규근사, 작으면 Knuth 방식)
func poisson(lambda float64) int {
	if lambda <= 0 {
		return 0
	}
	if lambda > 30 {
		// 정규 근사: N(lambda, lambda)
		z := rand.NormFloat64()
		v := int(math.Round(lambda + math.Sqrt(lambda)*z))
		if v < 0 {
			return 0
		}
		return v
	}
	// Knuth algorithm
	L := math.Exp(-lambda)
	k := 0
	p := 1.0
	for p > L {
		k++
		p *= rand.Float64()
	}
	return k - 1
}

// 로그정규 가중치 샘플: exp(N(mu, sigma^2))
func sampleLognormal(mu, sigma float64) float64 { return math.Exp(mu + sigma*rand.NormFloat64()) }

// 파레토 가중치 샘플: inverse-CDF
func samplePareto(alpha, xMin float64) float64 {
	u := rand.Float64()
	return xMin / math.Pow(1.0-u, 1.0/alpha)
}

// 누적합(CDF) 테이블 생성: 가중치 비율로 유저를 빨리 뽑기 위해 사용
func buildCDF(weights []float64) ([]float64, float64) {
	cdf := make([]float64, len(weights))
	sum := 0.0
	for i, w := range weights {
		sum += w
		cdf[i] = sum
	}
	return cdf, sum
}

// CDF에서 이진탐색으로 인덱스 하나 선택(가중치 비율에 비례)
func pickIndexFromCDF(cdf []float64, total float64) int {
	r := rand.Float64() * total
	lo, hi := 0, len(cdf)-1
	for lo < hi {
		mid := (lo + hi) >> 1
		if cdf[mid] >= r {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

/* ================================ main ================================ */

func main() {
	flag.Parse()
	// 매 실행마다 다른 랜덤 시드(동일 재현 원하면 고정값 사용)
	rand.Seed(time.Now().UnixNano())

	/* ---- 카프카 writer 준비 ---- */
	brokerList := strings.Split(*brokers, ",")
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        *topic,
		Balancer:     &kafka.LeastBytes{}, // 메시지 크기 기준으로 파티션 고르게
		BatchTimeout: 50 * time.Millisecond,
		// RequiredAcks: 기본(리더 ack). 더 강하게 하려면 kafka.RequireAll
	}
	defer w.Close()
	ctx := context.Background()

	/* ---- 헤비 유저 가중치 배열 만들기 ----
	   각 유저마다 '활동 강도'를 하나 뽑아둡니다.
	   가중치가 큰 유저는 이벤트를 더 자주 배정받습니다.
	*/
	weights := make([]float64, *users)
	for i := 0; i < *users; i++ {
		switch *dist {
		case "pareto":
			weights[i] = samplePareto(*alpha, *paretoMin)
		default: // "lognorm"
			weights[i] = sampleLognormal(*mu, *sigma)
		}
	}
	// 가중치 누적합(CDF) 준비: 이후 유저 뽑을 때 빠르게 사용
	cdf, sumW := buildCDF(weights)

	/* ---- 초당 기대값 계산 ----
	   -rate가 있으면 고정 초당 생성량 사용
	   아니면 users × base_per_user_per_min × 시간대/스파이크 보정 후 60으로 나눔
	*/
	now := time.Now()
	dow := now.Weekday()
	h := now.Hour()
	hBoost := hourBoost[h]
	sBoost := spikeBoost(dow, h)

	var expectedPerSec float64
	if *ratePerSec > 0 {
		expectedPerSec = *ratePerSec
	} else {
		expectedPerMin := float64(*users) * (*basePerUserPerMin) * hBoost * sBoost
		expectedPerSec = expectedPerMin / 60.0
	}

	/* ---- N건 모드: -n > 0 ----
	   duration과 관계없이 정확히 N건만 생성하고 종료합니다.
	   (빠르게 끝내기 위해 sleep 없이 배치 단위로 전송)
	*/
	if *nTotal > 0 {
		generateN(ctx, w, *nTotal, expectedPerSec, cdf, sumW)
		return
	}

	/* ---- duration 모드: 매 초 반복 ----
	   1) 이번 초 기대값을 살짝 흔들고(jitter)
	   2) 포아송으로 실제 개수를 뽑은 뒤
	   3) 그 개수만큼 유저를 (가중치 비율로) 골라 이벤트 생성
	   4) 카프카로 전송
	*/
	end := now.Add(*duration)
	total := 0
	for time.Now().Before(end) {
		// 초당 기대값에 ±비율로 약간의 랜덤을 곱함
		lam := jitterMul(expectedPerSec, *jitterRatio)

		// 이번 초 실제로 만들 개수(자연스러운 랜덤 출렁임)
		ev := poisson(lam)

		if ev > 0 {
			msgs := make([]kafka.Message, 0, ev)
			for i := 0; i < ev; i++ {
				// 유저 뽑기: 가중치가 큰 유저가 더 자주 선택됨
				uid := fmt.Sprintf("u_%d", 90000+pickIndexFromCDF(cdf, sumW))

				// 유저ID가 박힌 실제 이벤트 1건을 생성
				e := randomEventWithUser(uid)

				// JSON 직렬화
				b, err := marshal(e, *pretty)
				if err != nil {
					log.Fatalf("json marshal: %v", err)
				}

				// 보기 좋게도 출력하고(옵션), 카프카 메시지 목록에도 담음
				if *pretty {
					fmt.Println(string(b))
				}
				msgs = append(msgs, kafka.Message{Value: b})
			}

			// 배치로 전송 (성능)
			if err := w.WriteMessages(ctx, msgs...); err != nil {
				log.Fatalf("write messages: %v", err)
			}
			total += ev
		}

		// 실제 초 단위로 돌리려면 sleep 유지
		time.Sleep(1 * time.Second)
	}

	log.Printf("done: total events=%d (expected ~%.2f eps)", total, expectedPerSec)
}

/* N건 모드: 일정한 배치 크기로 뽑아 빠르게 전송 */
func generateN(ctx context.Context, w *kafka.Writer, n int, eps float64, cdf []float64, sumW float64) {
	total := 0
	for total < n {
		// 한 번에 보낼 배치 크기(평균 eps 정도, 최소 1)
		batch := poisson(math.Max(eps, 1))
		if batch == 0 {
			batch = 1
		}
		if total+batch > n {
			batch = n - total
		}

		msgs := make([]kafka.Message, 0, batch)
		for i := 0; i < batch; i++ {
			uid := fmt.Sprintf("u_%d", 90000+pickIndexFromCDF(cdf, sumW))
			e := randomEventWithUser(uid)
			// N모드에선 속도를 위해 pretty 생략(필요하면 바꿔도 OK)
			b, err := json.Marshal(e)
			if err != nil {
				log.Fatalf("json marshal: %v", err)
			}
			msgs = append(msgs, kafka.Message{Value: b})
		}

		if err := w.WriteMessages(ctx, msgs...); err != nil {
			log.Fatalf("write messages: %v", err)
		}
		total += batch
	}
	log.Printf("done: generated %d events (fixed-N)", total)
}

/* ======================= 이벤트 생성부 & 헬퍼들 ======================= */

// pretty 옵션에 따라 마샬링
func marshal(e Event, pretty bool) ([]byte, error) {
	if pretty {
		return json.MarshalIndent(e, "", "  ")
	}
	return json.Marshal(e)
}

// 주어진 userID를 박아 이벤트 한 건 생성
func randomEventWithUser(userID string) Event {
	now := time.Now().UTC()

	// 이벤트 발생 시각은 최근 90초 과거 ~ 30초 미래로 약간 퍼뜨림
	eventTime := now.Add(jitterDur(-90, 30))
	// 수집 지연(ingest)은 5~500ms
	ingestTime := eventTime.Add(jitterDur(5, 500))

	// 서비스/행동/페이지/상품ID 등 그럴싸한 값들 생성
	service := pick([]string{"web-frontend", "checkout", "catalog", "auth"}, nil)
	action := pick([]string{"pageview", "click", "view_item", "add_to_cart", "purchase"}, []int{40, 30, 15, 10, 5})
	page := randomPage(action)
	productID := pickProductID(page)

	// 디바이스/OS/UA
	device := pick([]string{"ios", "android", "web"}, []int{40, 40, 20})
	osName, osVer, ua, appVer := deviceProfile(device)

	// 성능/결과: 약 120ms를 중심으로 5~2000ms 범위
	latency := clippedNormalInt(120, 60, 5, 2000)
	status, ok := randomStatus()
	success := ok

	// 구매 이벤트면 금액(원)도 부여
	val := 0.0
	curr := "KRW"
	if action == "purchase" {
		val = toKRW(randNorm(35000, 20000, 1000, 500000)) // 1천원~50만원
	}

	ref := randomReferrer()
	utm := randomUTM()

	// 로그인 여부/익명ID/세션
	loggedIn := rand.Intn(100) < 75
	anonID := "anon_" + hexString(6)
	if loggedIn && userID == "" {
		userID = randomUserID()
	}

	return Event{
		EventID:    uuid4(),
		SchemaVer:  2,
		EventTime:  eventTime.Format(time.RFC3339Nano),
		IngestTime: ingestTime.Format(time.RFC3339Nano),
		Service:    service,
		TraceID:    hexString(16),
		SpanID:     hexString(8),

		UserID:       userID,
		AnonymousID:  anonID,
		UserLoggedIn: loggedIn,
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

// 액션에 따라 페이지 가중치 다르게
func randomPage(action string) string {
	paths := []string{
		"/", "/search?q=abc", "/search?q=shoes", "/category/men", "/category/women",
		"/product/42", "/product/77", "/cart", "/checkout",
	}
	switch action {
	case "purchase", "add_to_cart":
		return pick([]string{"/product/42", "/product/77", "/cart", "/checkout"}, []int{40, 30, 20, 10})
	case "view_item":
		return pick([]string{"/product/42", "/product/77"}, []int{60, 40})
	default:
		return pick(paths, nil)
	}
}

// 페이지 경로에 상품ID가 있으면 추출, 없으면 확률적으로 부여(빈값 포함)
func pickProductID(page string) string {
	if strings.HasPrefix(page, "/product/") {
		return strings.TrimPrefix(page, "/product/")
	}
	ids := []string{"", "42", "77", "13", "108"}
	return pick(ids, []int{50, 20, 15, 10, 5}) // 절반은 미지정
}

// 디바이스에 맞춰 OS/UA/App 버전 생성
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

// 상태코드: 성공 92%, 클라이언트 4%, 서버 4% 느낌
func randomStatus() (status int, ok bool) {
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

// referrer 랜덤
func randomReferrer() string {
	return pick(
		[]string{"/", "/search?q=abc", "/search?q=best+deal", "/category/men", "/category/women", ""},
		[]int{10, 30, 20, 15, 15, 10},
	)
}

// UTM 태그: 70%는 있음, 30%는 빈값
func randomUTM() [3]string {
	if rand.Intn(100) < 70 {
		src := pick([]string{"naver", "google", "kakao", "facebook", "newsletter"}, nil)
		med := pick([]string{"cpc", "organic", "email", "social"}, []int{50, 20, 15, 15})
		cmp := pick([]string{"fall_sale", "brand_kw", "retargeting", "weekly_digest"}, nil)
		return [3]string{src, med, cmp}
	}
	return [3]string{"", "", ""}
}

// 로그인 유저ID 랜덤
func randomUserID() string { return fmt.Sprintf("u_%d", 90000+rand.Intn(5000)) }

// UUID v4 간단 생성
func uuid4() string {
	b := make([]byte, 16)
	crand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// 임의의 바이트를 hex 문자열로
func hexString(nBytes int) string {
	b := make([]byte, nBytes)
	crand.Read(b)
	return hex.EncodeToString(b)
}

// 숫자문자열 n자리(세션ID 등)
func randomNumString(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteByte(byte('0' + rand.Intn(10)))
	}
	return sb.String()
}

// 밀리초 범위에서 랜덤 duration
func jitterDur(minMs, maxMs int) time.Duration {
	if maxMs < minMs {
		minMs, maxMs = maxMs, minMs
	}
	d := minMs + rand.Intn(maxMs-minMs+1)
	return time.Duration(d) * time.Millisecond
}

// 정규분포 샘플 후 정수/최소/최대 범위로 클리핑
func clippedNormalInt(mean, stddev, min, max int) int {
	return int(randNorm(float64(mean), float64(stddev), float64(min), float64(max)))
}

// 정규분포값 샘플 후 min~max로 자르기
func randNorm(mean, stddev, min, max float64) float64 {
	v := rand.NormFloat64()*stddev + mean
	if v < min {
		v = min
	}
	if v > max {
		v = max
	}
	return v
}

// 원 단위 반올림(100원 단위)
func toKRW(v float64) float64 { return float64(int(v/100.0+0.5)) * 100.0 }

// 가중치 있는 랜덤 선택(정수/일반 타입 모두)
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

func pickInt(vals []int, weights []int) int { return pick(vals, weights) }
