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
	OrderID           string  `json:"order_id"`
	Status            string  `json:"status"`
	AckedAtNs         int64   `json:"acked_at_ns"`
	ExpectedFillQty   int     `json:"expected_fill_qty"`
	ActualFillQty     int     `json:"actual_fill_qty"`
	ExpectedFillPrice float64 `json:"expected_fill_price"`
	ActualFillPrice   float64 `json:"actual_fill_price"`
	RejectReason      string  `json:"reject_reason"`
}

func main() {
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		var req OrderRequest
		json.NewDecoder(r.Body).Decode(&req)

		resp := OrderResponse{
			OrderID:           req.OrderID,
			Status:            "ack",
			AckedAtNs:         time.Now().UnixNano(),
			ExpectedFillQty:   req.Quantity,
			ActualFillQty:     req.Quantity,
			ExpectedFillPrice: req.Price,
			ActualFillPrice:   req.Price,
		}
		json.NewEncoder(w).Encode(resp)
	})

	log.Println("order server running on :9000")
	http.ListenAndServe(":9000", nil)
}