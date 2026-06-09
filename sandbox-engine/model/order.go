package model

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
	OrderID           string  `json:"order_id"`
	Status            string  `json:"status"` // ack | partial_fill | filled | rejected
	AckedAtNs         int64   `json:"acked_at_ns"`
	ExpectedFillQty   int     `json:"expected_fill_qty"`
	ActualFillQty     int     `json:"actual_fill_qty"`
	ExpectedFillPrice float64 `json:"expected_fill_price"`
	ActualFillPrice   float64 `json:"actual_fill_price"`
	RejectReason      string  `json:"reject_reason"` // null | insufficient_liquidity | invalid_price | invalid_order
}
