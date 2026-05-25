package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iicpc/bot-fleet/internal/bot"
	botrunner "github.com/iicpc/bot-fleet/internal/bot"
)

// TestBotFiresOrders spins up a local echo server and verifies
// the bot sends the expected number of orders.
func TestBotFiresOrders(t *testing.T) {
	var received atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/order" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		received.Add(1)
		var req bot.OrderRequest
		json.NewDecoder(r.Body).Decode(&req)

		resp := bot.OrderResponse{
			OrderID:   req.OrderID,
			Status:    "ack",
			AckedAtNs: time.Now().UnixNano(),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	var metrics []bot.BotMetrics
	cfg := botrunner.Config{
		BotID:        "test-bot-1",
		RunID:        "test-run-1",
		SubmissionID: "test-submission-1",
		TargetURL:    server.URL,
		Duration:     2 * time.Second,
		RatePerSec:   10,
	}

	ctx := context.Background()
	err := botrunner.Run(ctx, cfg, func(m bot.BotMetrics) {
		metrics = append(metrics, m)
	})
	if err != nil {
		t.Fatalf("bot.Run returned error: %v", err)
	}

	// 10 orders/sec × 2 sec = ~20, allow ±5 for timing variance
	count := received.Load()
	if count < 15 || count > 25 {
		t.Errorf("expected ~20 orders, got %d", count)
	}

	// All metrics should have non-zero latency
	for _, m := range metrics {
		if m.LatencyNs <= 0 {
			t.Errorf("expected positive latency, got %d for order %s", m.LatencyNs, m.OrderID)
		}
		if m.FillCorrect == false && m.OrderType != "cancel" {
			t.Logf("order %s type=%s fill_correct=false", m.OrderID, m.OrderType)
		}
	}

	t.Logf("fired %d orders, collected %d metrics", count, len(metrics))
}
