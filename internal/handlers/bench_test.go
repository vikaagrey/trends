package handlers_test

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vikagrej/trends/internal/topn"
)

func buildServer(t testing.TB, n int) *httptest.Server {
	t.Helper()
	items := make([]topn.Item, n)
	for i := range items {
		items[i] = topn.Item{Query: fmt.Sprintf("query-%d", i), Count: uint64(n - i)}
	}
	router := newTestRouter(items)
	return httptest.NewServer(router)
}

func BenchmarkHTTP_GetTop_Sequential(b *testing.B) {
	srv := buildServer(b, 100)
	defer srv.Close()

	url := srv.URL + "/api/v1/top?n=10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

func BenchmarkHTTP_GetTop_Parallel(b *testing.B) {
	srv := buildServer(b, 100)
	defer srv.Close()

	url := srv.URL + "/api/v1/top?n=10"
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := http.Get(url) //nolint:noctx
			if err != nil {
				b.Fatal(err)
			}
			resp.Body.Close()
		}
	})
}

func TestLoadStress_GetTop(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in -short mode")
	}

	const (
		clients  = 50
		duration = 3 * time.Second
	)

	srv := buildServer(t, 200)
	defer srv.Close()

	url := srv.URL + "/api/v1/top?n=10"

	transport := &http.Transport{
		MaxIdleConns:        clients * 2,
		MaxIdleConnsPerHost: clients * 2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	sharedClient := &http.Client{Transport: transport, Timeout: 2 * time.Second}

	var (
		totalReqs atomic.Int64
		totalErr  atomic.Int64
		mu        sync.Mutex
		latencies []time.Duration
	)

	start := time.Now()
	deadline := start.Add(duration)
	var wg sync.WaitGroup

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := sharedClient
			for time.Now().Before(deadline) {
				startedAt := time.Now()
				resp, err := client.Get(url)
				latency := time.Since(startedAt)

				if err != nil {
					totalErr.Add(1)
					continue
				}
				resp.Body.Close()
				totalReqs.Add(1)

				mu.Lock()
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	reqs := totalReqs.Load()
	errs := totalErr.Load()
	rps := float64(reqs) / elapsed.Seconds()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(math.Ceil(p/100*float64(len(latencies)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}

	t.Logf("GET /api/v1/top?n=10 load test")
	t.Logf("  clients:     %d", clients)
	t.Logf("  duration:    %s", elapsed.Round(time.Millisecond))
	t.Logf("  requests:    %d", reqs)
	t.Logf("  errors:      %d", errs)
	t.Logf("  RPS:         %.0f", rps)
	t.Logf("  p50 latency: %s", pct(50).Round(time.Microsecond))
	t.Logf("  p95 latency: %s", pct(95).Round(time.Microsecond))
	t.Logf("  p99 latency: %s", pct(99).Round(time.Microsecond))
	t.Logf("--------------------------------------------------------")

	if errs > 0 {
		t.Errorf("got %d errors out of %d requests", errs, reqs+errs)
	}
	const minRPS = 100
	if rps < minRPS {
		t.Errorf("RPS %.0f is below minimum threshold of %d", rps, minRPS)
	}
}

func TestResponse_Shape(t *testing.T) {
	items := []topn.Item{{Query: "golang", Count: 99}}
	router := newTestRouter(items)
	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/top?n=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var shape struct {
		Data      []map[string]any `json:"data"`
		Timestamp int64            `json:"timestamp"`
		WindowSec int              `json:"window_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&shape); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(shape.Data) != 1 {
		t.Errorf("expected 1 item, got %d", len(shape.Data))
	}
	if shape.WindowSec != 300 {
		t.Errorf("window_sec=%d, want 300", shape.WindowSec)
	}
	if shape.Timestamp <= 0 {
		t.Error("timestamp should be a positive unix epoch")
	}
	q, _ := shape.Data[0]["query"].(string)
	if q != "golang" {
		t.Errorf("query=%q, want golang", q)
	}
}
