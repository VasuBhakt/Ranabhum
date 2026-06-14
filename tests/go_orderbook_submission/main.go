package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
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

type Order struct {
	ID        string
	OrderType string
	Side      string
	Price     float64
	Quantity  int
	Timestamp int64
}

type OrderBook struct {
	mu   sync.Mutex
	bids []Order // buy orders, sorted best (highest) first
	asks []Order // sell orders, sorted best (lowest) first
}

func (ob *OrderBook) ProcessOrder(req OrderRequest) OrderResponse {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	now := time.Now().UnixNano()
	resp := OrderResponse{
		OrderID:           req.OrderID,
		AckedAtNs:         now,
		ExpectedFillQty:   req.Quantity,
		ExpectedFillPrice: req.Price,
	}

	newOrder := Order{
		ID:        req.OrderID,
		OrderType: req.OrderType,
		Side:      req.Side,
		Price:     req.Price,
		Quantity:  req.Quantity,
		Timestamp: now,
	}

	if req.OrderType == "cancel" {
		resp.Status = "ack"
		return resp
	}

	remainingQty := req.Quantity
	totalFillQty := 0
	totalFillValue := 0.0
	var matchedIDs []string

	if req.Side == "buy" {
		// Match against asks (sell orders) — lowest price first
		sort.Slice(ob.asks, func(i, j int) bool {
			if ob.asks[i].Price != ob.asks[j].Price {
				return ob.asks[i].Price < ob.asks[j].Price
			}
			return ob.asks[i].Timestamp < ob.asks[j].Timestamp
		})

		newAsks := make([]Order, 0, len(ob.asks))
		for _, ask := range ob.asks {
			if remainingQty <= 0 || (req.OrderType == "limit" && ask.Price > req.Price) {
				newAsks = append(newAsks, ask)
				continue
			}
			fillQty := min(remainingQty, ask.Quantity)
			remainingQty -= fillQty
			totalFillQty += fillQty
			totalFillValue += float64(fillQty) * ask.Price
			matchedIDs = append(matchedIDs, ask.ID)

			if ask.Quantity > fillQty {
				ask.Quantity -= fillQty
				newAsks = append(newAsks, ask)
			}
		}
		ob.asks = newAsks

		if totalFillQty > 0 {
			resp.ActualFillQty = totalFillQty
			resp.ActualFillPrice = math.Round(totalFillValue/float64(totalFillQty)*100) / 100
			resp.MatchedOrderIDs = matchedIDs
			if remainingQty == 0 {
				resp.Status = "filled"
			} else {
				resp.Status = "partial_fill"
			}
		}

		if req.OrderType == "limit" && remainingQty > 0 {
			newOrder.Quantity = remainingQty
			ob.bids = append(ob.bids, newOrder)
			if totalFillQty == 0 {
				resp.Status = "ack"
			}
		} else if req.OrderType == "market" && remainingQty > 0 && totalFillQty == 0 {
			resp.Status = "rejected"
		}
	} else {
		// Match against bids (buy orders) — highest price first
		sort.Slice(ob.bids, func(i, j int) bool {
			if ob.bids[i].Price != ob.bids[j].Price {
				return ob.bids[i].Price > ob.bids[j].Price
			}
			return ob.bids[i].Timestamp < ob.bids[j].Timestamp
		})

		newBids := make([]Order, 0, len(ob.bids))
		for _, bid := range ob.bids {
			if remainingQty <= 0 || (req.OrderType == "limit" && bid.Price < req.Price) {
				newBids = append(newBids, bid)
				continue
			}
			fillQty := min(remainingQty, bid.Quantity)
			remainingQty -= fillQty
			totalFillQty += fillQty
			totalFillValue += float64(fillQty) * bid.Price
			matchedIDs = append(matchedIDs, bid.ID)

			if bid.Quantity > fillQty {
				bid.Quantity -= fillQty
				newBids = append(newBids, bid)
			}
		}
		ob.bids = newBids

		if totalFillQty > 0 {
			resp.ActualFillQty = totalFillQty
			resp.ActualFillPrice = math.Round(totalFillValue/float64(totalFillQty)*100) / 100
			resp.MatchedOrderIDs = matchedIDs
			if remainingQty == 0 {
				resp.Status = "filled"
			} else {
				resp.Status = "partial_fill"
			}
		}

		if req.OrderType == "limit" && remainingQty > 0 {
			newOrder.Quantity = remainingQty
			ob.asks = append(ob.asks, newOrder)
			if totalFillQty == 0 {
				resp.Status = "ack"
			}
		} else if req.OrderType == "market" && remainingQty > 0 && totalFillQty == 0 {
			resp.Status = "rejected"
		}
	}

	// If status is still empty (market order that filled partially), mark as partial_fill
	if resp.Status == "" && totalFillQty > 0 {
		resp.Status = "partial_fill"
	} else if resp.Status == "" {
		resp.Status = "ack"
	}

	return resp
}

var book = &OrderBook{}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	resp := book.ProcessOrder(req)

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

	log.Println("Go Order Matching Engine (with full order book) listening on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
