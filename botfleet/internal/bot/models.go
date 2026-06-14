package bot

import "github.com/google/uuid"

// SubmissionReady is the event consumed from the submission.ready Kafka topic.
// Published by MS1 (submission-engine), consumed by MS2 (bot-fleet).
type SubmissionReady struct {
	SubmissionID string `json:"submission_id"`
	ContestantID string `json:"contestant_id"`
	EndpointURL  string `json:"endpoint_url"`
	Port         int    `json:"port"`
	Language     string `json:"language"`        // cpp | rust | go
	SubmittedAt  string `json:"submitted_at"`    // ISO8601
	CPULimit     int    `json:"cpu_limit"`       // default 2
	MemoryLimit  int    `json:"memory_limit_mb"` // mb
	Status       string `json:"status"`          // uploaded | building | running | stress_testing | scored | failed
	ContainerID  string `json:"container_id"`
}

// BotMetrics is the event published to the bot.metrics Kafka topic.
// Consumed by MS3 (telemetry).
type BotMetrics struct {
	SubmissionID      string  `json:"submission_id"`
	RunID             string  `json:"run_id"`
	BotID             string  `json:"bot_id"`
	OrderID           string  `json:"order_id"`
	CancelOrderID     string  `json:"cancel_order_id"`
	OrderType         string  `json:"order_type"` // limit | market | cancel
	Side              string  `json:"side"`       // buy | sell | null
	Price             float64 `json:"price"`
	Quantity          int     `json:"quantity"`
	SentAtNs          int64   `json:"sent_at_ns"`
	AckAtNs           int64   `json:"ack_at_ns"`
	LatencyNs         int64   `json:"latency_ns"`
	ExpectedFillQty   int     `json:"expected_fill_qty"`
	ActualFillQty     int     `json:"actual_fill_qty"`
	ExpectedFillPrice float64 `json:"expected_fill_price"`
	ActualFillPrice   float64 `json:"actual_fill_price"`
	FillCorrect       int     `json:"fill_correct"`
	Status            string  `json:"status"`        // ack | partial_fill | filled | rejected | timeout
	RejectReason      string  `json:"reject_reason"` // null | insufficient_liquidity | invalid_price | invalid_order
}

// OrderRequest is the HTTP body sent to the MS1 sandbox REST endpoint.
type OrderRequest struct {
	OrderID       string  `json:"order_id"`
	SubmissionID  string  `json:"submission_id"`
	OrderType     string  `json:"order_type"` // limit | market | cancel
	Side          string  `json:"side"`       // buy | sell | null
	Price         float64 `json:"price"`      // ignored for market orders
	Quantity      int     `json:"quantity"`
	CancelOrderID string  `json:"cancel_order_id"`
	TimestampNS   int64   `json:"timestamp_ns"`
}

// OrderResponse is the HTTP response from the MS1 sandbox.
type OrderResponse struct {
	OrderID           string   `json:"order_id"`
	Status            string   `json:"status"` // ack | partial_fill | filled | rejected
	AckedAtNs         int64    `json:"acked_at_ns"`
	ExpectedFillQty   int      `json:"expected_fill_qty"`
	ActualFillQty     int      `json:"actual_fill_qty"`
	ExpectedFillPrice float64  `json:"expected_fill_price"`
	ActualFillPrice   float64  `json:"actual_fill_price"`
	RejectReason      string   `json:"reject_reason"`               // null | insufficient_liquidity | invalid_price | invalid_order
	MatchedOrderIDs   []string `json:"matched_order_ids,omitempty"` // Added for Certification Phase
}

// RunState tracks the lifecycle of a stress-test run in Redis.
type RunState struct {
	RunID              string  `json:"run_id"`
	SubmissionID       string  `json:"submission_id"`
	ContestantID       string  `json:"contestant_id"`
	Status             string  `json:"status"` // PENDING | RUNNING | DONE | FAILED
	BotCount           int     `json:"bot_count"`
	StartedAt          int64   `json:"started_at_ns"`
	EndedAt            int64   `json:"ended_at_ns,omitempty"`
	CertificationScore float64 `json:"certification_score"` // 0.0 to 1.0, or -1.0 if not attempted
}

// NewOrderRequest creates an OrderRequest with a generated ID and current timestamp.
func NewOrderRequest(orderType, side string, price float64, quantity int) OrderRequest {
	return OrderRequest{
		OrderID:     uuid.NewString(),
		OrderType:   orderType,
		Side:        side,
		Price:       price,
		Quantity:    quantity,
		TimestampNS: 0, // caller should set this to time.Now().UnixNano()
	}
}
