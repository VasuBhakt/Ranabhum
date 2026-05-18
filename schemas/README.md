# API & Schema Standards

Defines the strict data contracts (FIX, REST, and internal gRPC/WebSocket formats) that ensure every component (Bot Fleet, Exchange, Telemetry) follows common standard and structure.

## Maintainer : All maintainers. Changes require consensus of all.

## Schemas defined and models to be used within the services:

1. `submission_ready.json`:

Contestant uploads code via UI
↓
MS1 builds + containerizes it
↓
MS1 publishes submission_ready to Redpanda
↓
MS2 consumes it → knows where to send bot traffic

One event per submission. MS2 just waits on this to know `endpoint_url` and `port` to start firing orders.

---

2. `order_request.json`:

MS2 bot generates an order
↓
MS2 POST /order → MS1 sandbox directly (HTTP)
↓
MS1 matching engine processes it
↓
MS1 returns order_response

Thousands of such order_request.json files are generated per second, no message broker involved due to latency concerns. Direct HTTP.

---

3. `order_response.json`:

MS1 returns response to MS2 bot
↓
MS2 captures ack_at_ns
↓
MS2 computes latency_ns = ack_at_ns - sent_at_ns
↓
MS2 compares actual_fill vs expected_fill
↓
MS2 sets fill_correct = true/false

Never leaves MS2 as its own message, gets absorbed into `bot_metrics.json`

---

4. `bot_metrics.json`:

MS2 assembles order_request + order_response into one event
↓
MS2 publishes to bot.metrics topic on Redpanda
↓
MS3 consumes it → writes raw row to TimescaleDB
↓
MS3 scoring engine runs window functions
→ p50/p90/p99 latency
→ TPS
→ correctness_rate
↓
MS3 writes composite score to Redis sorted set
↓
MS3 pushes update via WebSocket
↓
React leaderboard updates live

---

MS1 : Microservice 1 (Sandboxing Engine)
MS2 : Microservice 2 (Bot Fleet)
MS3 : Microservice 3 (Telemetry Engine)
