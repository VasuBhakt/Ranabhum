import json
import asyncio
import asyncpg
import urllib.request
import redis.asyncio as redis
import os
from aiokafka import AIOKafkaConsumer
from app.scoring.engine import compute_scores
from app.db.redis_store import update_leaderboard

# tracks the last time we saw a message for each run_id
run_last_seen = {}
# tracks whether we've already scheduled a score for this run
run_score_tasks = {}
# batch buffer for inserts
batch_buffer = []
BATCH_SIZE = 5000
batch_counter = {}  # track batches per run_id

async def flush_batch(db_pool, run_id):
    """Insert batched messages to DB."""
    if not batch_buffer:
        return
    
    # Synchronously copy and clear the buffer to prevent race conditions
    to_insert = list(batch_buffer)
    batch_buffer.clear()
    
    num_messages = len(to_insert)
    if run_id not in batch_counter:
        batch_counter[run_id] = 0
    batch_counter[run_id] += 1
    
    async def _do_insert():
        # Acquire a connection from the pool for concurrent execution
        async with db_pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO order_metrics 
                (submission_id, run_id, order_id, bot_id, cancel_order_id, order_type, side,
                 price, quantity, sent_at, ack_at_ns, latency_ns, expected_fill_qty,
                 actual_fill_qty, expected_fill_price, actual_fill_price, fill_correct,
                 status, reject_reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                        to_timestamp($10::double precision / 1e9), $11, $12, $13,
                        $14, $15, $16, $17, $18, $19)
            """, to_insert)

    # Shield the database insert from task cancellation to prevent connection issues or data loss
    await asyncio.shield(_do_insert())
    print(f"📦 Run {run_id[:8]}... Batch #{batch_counter[run_id]} inserted {num_messages} messages")


async def score_after_delay(sub_id, run_id, db_pool, redis_client, delay=5):
    """Wait `delay` seconds after last message, then compute final score."""
    await asyncio.sleep(delay)
    
    # if a newer message came in during sleep, this task is stale — abort
    if run_last_seen.get(run_id, 0) > asyncio.get_event_loop().time() - delay:
        return
    
    # Flush any remaining messages in the buffer before scoring
    await flush_batch(db_pool, run_id)
    
    print(f"\n✅ Run {run_id} complete, computing final score...")
    new_score = await compute_scores(sub_id, run_id, db_pool=db_pool, redis_client=redis_client)
    await update_leaderboard(redis_client, new_score)
    print(f"🏆 Final score for {sub_id}: {new_score['score']}")
    
    # broadcast to React
    try:
        api_url = os.getenv("TELEMETRY_API_URL", "http://localhost:8001")
        req = urllib.request.Request(
            f"{api_url}/broadcast",
            data=json.dumps(new_score).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req)
        print("📢 Broadcasted final score to React!")
    except Exception as e:
        print(f"⚠️ Could not broadcast: {e}")
    
    # cleanup
    run_last_seen.pop(run_id, None)
    run_score_tasks.pop(run_id, None)

async def consume_metrics():
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    
    # Connection retry loop for database pool
    db_pool = None
    for attempt in range(15):
        try:
            db_pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
            print("💾 Connected to TimescaleDB Connection Pool successfully")
            break
        except Exception as e:
            print(f"Waiting for TimescaleDB to be ready (attempt {attempt + 1}/15)...")
            await asyncio.sleep(2)
    if db_pool is None:
        raise Exception("Could not connect to database pool after 15 attempts")

    # Auto-initialize database schema if not present
    try:
        async with db_pool.acquire() as pg_conn:
            table_exists = await pg_conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'order_metrics'
                )
            """)
            if not table_exists:
                print("🧱 Table 'order_metrics' not found. Initializing schema...")
                schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "schema.sql")
                with open(schema_path, "r") as f:
                    schema_sql = f.read()
                # Split by semicolon and execute non-empty statements
                for statement in schema_sql.split(";"):
                    stmt = statement.strip()
                    if stmt:
                        await pg_conn.execute(stmt)
                print("✅ Database schema initialized successfully")
            
            # Ensure composite index is created
            await pg_conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_submission_run
                ON order_metrics (submission_id, run_id, sent_at DESC)
            """)
            print("⚡ Ensured composite run index exists")
    except Exception as e:
        print(f"⚠️ Failed to auto-initialize database schema: {e}")

    # Connection retry loop for Redis
    redis_client = await redis.from_url(redis_url)
    for attempt in range(15):
        try:
            await redis_client.ping()
            print("🧠 Connected to Redis successfully")
            break
        except Exception as e:
            print(f"Waiting for Redis to be ready (attempt {attempt + 1}/15)...")
            await asyncio.sleep(2)

    consumer = AIOKafkaConsumer(
        "bot.metrics",
        bootstrap_servers=kafka_brokers,
        group_id="telemetry-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )
    
    # Connection retry loop for Kafka/Redpanda
    for attempt in range(15):
        try:
            await consumer.start()
            print("⚡ Connected to Redpanda successfully")
            break
        except Exception as e:
            print(f"Waiting for Redpanda to be ready (attempt {attempt + 1}/15)...")
            await asyncio.sleep(2)
    print("⚡ Live Pipeline Started! Waiting for bot telemetry...")
    
    try:
        async for message in consumer:
            data = message.value
            sub_id = data["submission_id"]
            run_id = data["run_id"]
            
            # add to batch buffer with all fields
            batch_buffer.append((
                sub_id, run_id, data["order_id"], data["bot_id"],
                data.get("cancel_order_id"), data["order_type"], data.get("side"),
                data.get("price"), data.get("quantity"),
                data["sent_at_ns"], data.get("ack_at_ns"), data["latency_ns"],
                data.get("expected_fill_qty"), data.get("actual_fill_qty"),
                data.get("expected_fill_price"), data.get("actual_fill_price"),
                data.get("fill_correct"), data.get("status"), data.get("reject_reason")
            ))
            
            # flush batch if we hit batch size
            if len(batch_buffer) >= BATCH_SIZE:
                await flush_batch(db_pool, run_id)
            
            # update last seen time for this run
            run_last_seen[run_id] = asyncio.get_event_loop().time()
            
            # cancel existing score task for this run if any
            if run_id in run_score_tasks:
                run_score_tasks[run_id].cancel()
            
            # schedule a new score task — will fire 5s after last message
            run_score_tasks[run_id] = asyncio.create_task(
                score_after_delay(sub_id, run_id, db_pool, redis_client, delay=5)
            )
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # flush remaining messages - extract run_id from buffer if available
        if batch_buffer:
            run_id = batch_buffer[0][1]  # run_id is second element in tuple
            await flush_batch(db_pool, run_id)
        await consumer.stop()
        if db_pool:
            await db_pool.close()
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(consume_metrics())