package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Config holds all the settings for a single bot instance.
// One Config is created per bot, per submission run.
// Example: 10 bots per submission = 10 Config objects, all with the same
// SubmissionID and RunID but different BotIDs.
type Config struct {
	BotID        string        // unique ID for this bot instance
	RunID        string        // ID for this stress-test run (one per submission execution)
	SubmissionID string        // ID of the contestant's submission being tested
	TargetURL    string        // base URL of the MS1 sandbox, e.g. http://sandbox-abc.internal:8080
	Duration     time.Duration // how long this bot should run before stopping
	RatePerSec   int           // how many orders per second this bot should fire
}

// MetricsCallback is a function that receives the result of each order.
// The coordinator passes in a function that publishes to Kafka —
// this keeps the bot logic decoupled from Kafka entirely.
type MetricsCallback func(m BotMetrics)

// Run starts a single bot and blocks until ctx is cancelled or Duration expires.
// It fires orders at RatePerSec orders/second by using a ticker.
// Each order is fired in a separate goroutine (go fireOrder) so they don't
// block each other — this is how we achieve high concurrency.
func Run(ctx context.Context, cfg Config, onMetric MetricsCallback) error {
	client := &http.Client{Timeout: 5 * time.Second}

	// ticker fires once every (1/RatePerSec) seconds
	// e.g. RatePerSec=50 means ticker fires every 20ms
	ticker := time.NewTicker(time.Second / time.Duration(cfg.RatePerSec))
	defer ticker.Stop()

	// deadline is when this bot should stop firing orders
	deadline := time.Now().Add(cfg.Duration)

	for {
		select {
		case <-ctx.Done():
			// context was cancelled (e.g. coordinator is shutting down)
			// return cleanly without an error
			return ctx.Err()
		case t := <-ticker.C:
			// ticker fired — time to send another order
			if t.After(deadline) {
				// we've run for the full duration, stop cleanly
				return nil
			}
			// fire the order in a goroutine so we don't block the ticker
			// this means multiple orders can be in-flight at the same time
			go fireOrder(ctx, client, cfg, onMetric)
		}
	}
}

// fireOrder sends a single order to the sandbox and reports the result.
// It is called in a goroutine by Run() so many of these run concurrently.
// Steps:
//  1. generate a random order
//  2. send HTTP POST /order to the sandbox
//  3. measure round-trip latency in nanoseconds
//  4. parse the response and check correctness
//  5. call onMetric() with the full result (which publishes to Kafka)
func fireOrder(ctx context.Context, client *http.Client, cfg Config, onMetric MetricsCallback) {
	// generate a random order (limit/market/cancel with realistic distribution)
	order := randomOrder()

	// convert the order struct to JSON bytes for the HTTP body
	payload, err := json.Marshal(order)
	if err != nil {
		return
	}

	url := fmt.Sprintf("%s/order", cfg.TargetURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// capture timestamp just before sending — this is the start of latency measurement
	sentAt := time.Now().UnixNano()
	resp, err := client.Do(req)
	// capture timestamp immediately after response — end of latency measurement
	ackAt := time.Now().UnixNano()

	// default values if the request fails or sandbox returns garbage
	status := "timeout"
	rejectReason := ""
	fillCorrect := 0
	var expectedFillQty, actualFillQty int
	var expectedFillPrice, actualFillPrice float64

	if err == nil {
		defer resp.Body.Close()
		var orderResp OrderResponse
		if json.NewDecoder(resp.Body).Decode(&orderResp) == nil {
			status = orderResp.Status
			rejectReason = orderResp.RejectReason
			expectedFillQty = orderResp.ExpectedFillQty
			actualFillQty = orderResp.ActualFillQty
			expectedFillPrice = orderResp.ExpectedFillPrice
			actualFillPrice = orderResp.ActualFillPrice

			// order is considered correct if it was acked, partially filled, or fully filled
			// rejected or timeout = incorrect
			if (status == "ack" || status == "partial_fill" || status == "filled") {
				fillCorrect = 1;
			}
		}
	}

	// report the full result back to the coordinator via the callback
	// the coordinator will publish this to the bot.metrics Kafka topic
	onMetric(BotMetrics{
		SubmissionID:      cfg.SubmissionID,
		RunID:             cfg.RunID,
		BotID:             cfg.BotID,
		OrderID:           order.OrderID,
		CancelOrderID:     order.CancelOrderID,
		OrderType:         order.OrderType,
		Side:              order.Side,
		Price:             order.Price,
		Quantity:          order.Quantity,
		SentAtNs:          sentAt,
		AckAtNs:           ackAt,
		LatencyNs:         ackAt - sentAt, // total round-trip in nanoseconds
		ExpectedFillQty:   expectedFillQty,
		ActualFillQty:     actualFillQty,
		ExpectedFillPrice: expectedFillPrice,
		ActualFillPrice:   actualFillPrice,
		FillCorrect:       fillCorrect,
		Status:            status,
		RejectReason:      rejectReason,
	})
}

// randomOrder generates a single realistic order with randomised fields.
// Order type distribution mirrors real market behaviour:
//   - 60% limit orders  (most common in real markets)
//   - 30% market orders (immediate execution)
//   - 10% cancel orders (cancelling a previous order)
//
// For cancel orders, side is set to "null" since it doesn't apply.
// Price range is 100–150 to simulate a realistic instrument price.
func randomOrder() OrderRequest {
	r := rand.Float64()
	var orderType string
	var side string
	var cancelOrderID string

	switch {
	case r < 0.60:
		orderType = "limit"
	case r < 0.90:
		orderType = "market"
	default:
		orderType = "cancel"
	}

	if orderType == "cancel" {
		// cancel orders don't have a side — they reference a previous order
		side = "null"
		cancelOrderID = uuid.NewString() // in real usage this would be a real order ID
	} else {
		// randomly pick buy or sell with equal probability
		if rand.Float64() > 0.5 {
			side = "buy"
		} else {
			side = "sell"
		}
	}

	return OrderRequest{
		OrderID:       uuid.NewString(),
		OrderType:     orderType,
		Side:          side,
		Price:         100.0 + rand.Float64()*50.0, // random price between 100 and 150
		Quantity:      1 + rand.Intn(100),          // random quantity between 1 and 100
		CancelOrderID: cancelOrderID,
		TimestampNS:   time.Now().UnixNano(),
	}
}
