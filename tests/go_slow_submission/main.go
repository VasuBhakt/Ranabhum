package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type OrderRequest struct {
	OrderID   string  `json:"order_id"`
	OrderType string  `json:"order_type"`
	Side      string  `json:"side"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
}

type OrderResponse struct {
	OrderID           string   `json:"order_id"`
	Status            string   `json:"status"`
	AckedAtNs         int64    `json:"acked_at_ns"`
	ExpectedFillQty   int      `json:"expected_fill_qty"`
	ActualFillQty     int      `json:"actual_fill_qty"`
	ExpectedFillPrice float64  `json:"expected_fill_price"`
	ActualFillPrice   float64  `json:"actual_fill_price"`
	RejectReason      string   `json:"reject_reason"`
	MatchedOrderIDs   []string `json:"matched_order_ids,omitempty"`
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Simulate slow processing — 2ms per order
	time.Sleep(2 * time.Millisecond)

	resp := OrderResponse{
		OrderID:           req.OrderID,
		Status:            "ack",
		AckedAtNs:         time.Now().UnixNano(),
		ExpectedFillQty:   req.Quantity,
		ActualFillQty:     req.Quantity,
		ExpectedFillPrice: req.Price,
		ActualFillPrice:   req.Price,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func main() {
	server := &http.Server{
		Addr:         ":8080",
		Handler:      http.HandlerFunc(handleOrder),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	log.Println("Slow Go Order Engine (2ms latency) listening on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
