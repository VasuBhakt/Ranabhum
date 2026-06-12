package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
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

// buffer pool to reduce GC pressure
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 256)
	},
}

var requestCount uint64

func handleOrder(w http.ResponseWriter, r *http.Request) {
	// parse request
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	status := "ack"
	var rejectReason string
	if atomic.AddUint64(&requestCount, 1)%10 == 0 {
		status = "rejected"
		rejectReason = "simulated rejection"
	}

	// build response with minimal allocations
	resp := OrderResponse{
		OrderID:           req.OrderID,
		Status:            status,
		AckedAtNs:         time.Now().UnixNano(),
		ExpectedFillQty:   req.Quantity,
		ActualFillQty:     req.Quantity,
		ExpectedFillPrice: req.Price,
		ActualFillPrice:   req.Price,
		RejectReason:      rejectReason,
	}

	// set headers
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "keep-alive")

	// encode response
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("encode error: %v", err)
	}
}

func main() {
	// use keep-alive enabled server for connection reuse
	server := &http.Server{
		Addr:         ":8080",
		Handler:      http.HandlerFunc(handleOrder),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	log.Println("optimized order server running on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
