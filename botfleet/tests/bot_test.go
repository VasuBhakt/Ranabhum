package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	bot "Ranabhum/bot-fleet/internal/bot"
)

func TestBotFiresOrders(t *testing.T) {
	var received atomic.Int64
	var limitCount, marketCount, cancelCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/order" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		received.Add(1)

		var req bot.OrderRequest
		json.NewDecoder(r.Body).Decode(&req)

		// count order types
		switch req.OrderType {
		case "limit":
			limitCount.Add(1)
		case "market":
			marketCount.Add(1)
		case "cancel":
			cancelCount.Add(1)
		}

		t.Logf("[server] received order_id=%s type=%s side=%s price=%.2f qty=%d",
			req.OrderID, req.OrderType, req.Side, req.Price, req.Quantity)

		resp := bot.OrderResponse{
			OrderID:           req.OrderID,
			Status:            "ack",
			AckedAtNs:         time.Now().UnixNano(),
			ExpectedFillQty:   req.Quantity,
			ActualFillQty:     req.Quantity,
			ExpectedFillPrice: req.Price,
			ActualFillPrice:   req.Price,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	t.Logf("[test] echo server started at %s", server.URL)

	var metrics []bot.BotMetrics
	cfg := bot.Config{
		BotID:        "test-bot-1",
		RunID:        "test-run-1",
		SubmissionID: "test-submission-1",
		TargetURL:    server.URL,
		Duration:     2 * time.Second,
		RatePerSec:   10,
	}

	t.Logf("[test] starting bot — rate=%d/s duration=%s", cfg.RatePerSec, cfg.Duration)

	ctx := context.Background()
	err := bot.Run(ctx, cfg, func(m bot.BotMetrics) {
		metrics = append(metrics, m)
		t.Logf("[metric] order_id=%s type=%s status=%s latency=%dns fill_correct=%v",
			m.OrderID, m.OrderType, m.Status, m.LatencyNs, m.FillCorrect)
	})
	if err != nil {
		t.Fatalf("bot.Run returned error: %v", err)
	}

	t.Logf("--------- SUMMARY ---------")
	t.Logf("total orders fired:  %d", received.Load())
	t.Logf("limit orders:        %d", limitCount.Load())
	t.Logf("market orders:       %d", marketCount.Load())
	t.Logf("cancel orders:       %d", cancelCount.Load())
	t.Logf("metrics collected:   %d", len(metrics))

	// calculate average latency
	var totalLatency int64
	var correctCount int
	for _, m := range metrics {
		totalLatency += m.LatencyNs
		if m.FillCorrect {
			correctCount++
		}
	}
	if len(metrics) > 0 {
		t.Logf("avg latency:         %dns", totalLatency/int64(len(metrics)))
		t.Logf("fill correct:        %d/%d", correctCount, len(metrics))
	}
	t.Logf("---------------------------")

	// assertions
	count := received.Load()
	if count < 15 || count > 25 {
		t.Errorf("expected ~20 orders, got %d", count)
	}
	if int(count) != len(metrics) {
		t.Errorf("orders fired=%d but metrics collected=%d — mismatch", count, len(metrics))
	}
}
