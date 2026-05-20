# MS3 — Telemetry, Scoring & Leaderboard

**Owner: Dev C**  
**Stack: Python (FastAPI) + TimescaleDB + Redis + React**  
**Port: 8001 (API) | 5173 (Frontend)**

---

## What this service does

This is the **data + scoring + leaderboard** microservice. It:

1. **Listens** to the `bot.metrics` Kafka topic (published by Dev B's bot fleet)
2. **Stores** every order metric into TimescaleDB
3. **Computes** scores — p50/p90/p99 latency, TPS, correctness rate
4. **Broadcasts** live scores to the React leaderboard via WebSocket

---

## For Dev A (Submission Engine) — what you need to know

MS3 does **not** directly depend on MS1. You don't need to do anything special for integration.

MS3 only reads from Kafka. As long as MS1 correctly publishes to `submission.ready` and Dev B's bots start firing, MS3 picks it up automatically.

The only thing MS3 uses from MS1 indirectly is the `submission_id` field — make sure it's a consistent string (UUID v4 recommended) because MS3 groups the leaderboard by it.

---

## For Dev B (Bot Fleet) — CRITICAL — read this carefully

MS3 consumes from your `bot.metrics` Kafka topic. Your bots **must publish messages in exactly this JSON format** or MS3 will drop them:

```json
{
  "submission_id": "test-123",
  "run_id": "run-1",
  "order_id": "ord-1",
  "order_type": "limit",
  "sent_at_ns": 1715000000000000000,
  "ack_at_ns": 1715000000001200000,
  "latency_ns": 1200000,
  "fill_correct": true,
  "bot_id": "bot-A"
}
```

### Field definitions

| Field | Type | Description |
|---|---|---|
| `submission_id` | string | ID of the contestant submission (from MS1) |
| `run_id` | string | ID of the current stress-test run |
| `order_id` | string | Unique ID for this individual order |
| `order_type` | string | One of: `limit`, `market`, `cancel` |
| `sent_at_ns` | integer | Unix timestamp in **nanoseconds** when order was sent |
| `ack_at_ns` | integer | Unix timestamp in **nanoseconds** when ack was received |
| `latency_ns` | integer | `ack_at_ns - sent_at_ns` (nanoseconds) |
| `fill_correct` | boolean | Whether the order was filled correctly |
| `bot_id` | string | ID of the bot that sent this order |

### How to test your messages reach MS3

Publish a test message manually to verify the pipeline works:

```bash
echo '{"submission_id":"test-123","run_id":"run-1","order_id":"ord-1","order_type":"limit","sent_at_ns":1715000000000000000,"ack_at_ns":1715000000001200000,"latency_ns":1200000,"fill_correct":true,"bot_id":"bot-A"}' | docker exec -i redpanda rpk topic produce bot.metrics
```

Then check `http://localhost:8001/leaderboard` — you should see `test-123` appear within 2 seconds.

---

## How to run MS3 on your machine

### Prerequisites

- Python 3.10+
- Node.js 18+
- Docker Desktop (running)

### Step 1 — Clone the repo and go to telemetry folder

```bash
cd iicpc-platform/telemetry
```

### Step 2 — Create virtual environment and install dependencies

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### Step 3 — Start shared infrastructure (if not already running)

```bash
# From the root of the monorepo
docker compose up -d
```

Or start individually:

```bash
docker start timescaledb redis redpanda
```

If containers don't exist yet:

```bash
docker run -d --name timescaledb -e POSTGRES_PASSWORD=postgres -p 5432:5432 timescale/timescaledb:latest-pg16
docker run -d --name redis -p 6379:6379 redis:alpine
docker run -d --name redpanda -p 9092:9092 -p 9644:9644 redpandadata/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 512M --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://localhost:9092
```

### Step 4 — Create the database table

**Windows:**
```bash
Get-Content app\db\schema.sql | docker exec -i timescaledb psql -U postgres
```

**Mac/Linux:**
```bash
docker exec -i timescaledb psql -U postgres < app/db/schema.sql
```

Expected output:
```
CREATE EXTENSION
CREATE TABLE
 create_hypertable
-------------------
(1 row)
CREATE INDEX
```

### Step 5 — Start the API server

```bash
uvicorn app.main:app --reload --port 8001
```

### Step 6 — Start the React frontend (second terminal)

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173` — you'll see the Live Hackathon Leaderboard.

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Check if service + DB are running |
| GET | `/leaderboard` | Get current leaderboard as JSON |
| WebSocket | `/ws/leaderboard` | Live score updates pushed to browser |
| POST | `/broadcast` | Internal — used by consumer to push updates |

### Example leaderboard response

```json
[
  {
    "submission_id": "test-123",
    "score": 85.5,
    "p50_ms": 0.5,
    "p90_ms": 1.2,
    "p99_ms": 2.0,
    "tps": 500,
    "correctness": 1.0
  }
]
```

---

## Scoring formula

```
composite_score = (0.4 × TPS) + (0.4 × (1 / p99_ms)) + (0.2 × correctness_rate)
```

Higher score = better submission. Lower latency and higher TPS wins.

---

## Project structure

```
telemetry/
├── app/
│   ├── main.py              ← FastAPI app, all endpoints
│   ├── consumer/
│   │   └── kafka_consumer.py  ← reads bot.metrics from Redpanda
│   ├── scoring/
│   │   └── engine.py          ← computes p50/p90/p99, TPS, score
│   ├── ws/
│   │   └── broadcaster.py     ← WebSocket to push scores to React
│   └── db/
│       └── schema.sql         ← TimescaleDB table definition
├── frontend/
│   └── src/
│       └── App.tsx            ← React leaderboard UI
├── requirements.txt
└── .env.example
```

---

## Environment variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env   # Mac/Linux
copy .env.example .env  # Windows
```

`.env.example`:
```
DATABASE_URL=postgresql://postgres@localhost:5432/postgres
REDIS_URL=redis://localhost:6379
KAFKA_BROKER=localhost:9092
```

---

## Daily startup (after PC restart)

```bash
# 1. Start Docker Desktop and wait for it to load

# 2. Start containers
docker start timescaledb redis redpanda

# 3. Terminal 1 — start API
cd iicpc-platform/telemetry
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
uvicorn app.main:app --reload --port 8001

# 4. Terminal 2 — start frontend
cd iicpc-platform/telemetry/frontend
npm run dev
```

---

## Troubleshooting

**Consumer not receiving messages?**
- Check Redpanda is running: `docker ps`
- Verify the topic exists: `docker exec -i redpanda rpk topic list`
- Create it if missing: `docker exec -i redpanda rpk topic create bot.metrics`

**Database connection error?**
- Make sure timescaledb container is running: `docker start timescaledb`
- Test connection: `docker exec -it timescaledb psql -U postgres -c "SELECT 1"`

**Leaderboard empty?**
- Publish a test message (see Dev B section above)
- Check consumer terminal for "Received order from..." logs

**Port already in use?**
- API uses port `8001` — make sure nothing else is on it
- Frontend uses port `5173`

---

## What's next (Week 2 integration)

When Dev A and Dev B push their services, the only change needed is making sure:

1. Redpanda is the shared message broker (all three services point to same `KAFKA_BROKER`)
2. Dev B publishes to `bot.metrics` with the exact schema above
3. The `submission_id` in `bot.metrics` matches what MS1 generated

No code changes needed on MS3 side for basic integration. 🎉
