import json
import asyncio
import asyncpg
import urllib.request
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer
from app.scoring.engine import compute_scores
from app.db.redis_store import update_leaderboard

# tracks the last time we saw a message for each run_id
run_last_seen = {}
# tracks whether we've already scheduled a score for this run
run_score_tasks = {}
# batch buffer for inserts
batch_buffer = []
BATCH_SIZE = 100
batch_counter = {}  # track batches per run_id

async def flush_batch(pg_conn, run_id):
    """Insert batched messages to DB."""
    if not batch_buffer:
        return
    num_messages = len(batch_buffer)
    if run_id not in batch_counter:
        batch_counter[run_id] = 0
    batch_counter[run_id] += 1
    await pg_conn.executemany("""
        INSERT INTO order_metrics 
        (submission_id, run_id, order_id, bot_id, cancel_order_id, order_type, side,
         price, quantity, sent_at, ack_at_ns, latency_ns, expected_fill_qty,
         actual_fill_qty, expected_fill_price, actual_fill_price, fill_correct,
         status, reject_reason)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                to_timestamp($10::double precision / 1e9), $11, $12, $13,
                $14, $15, $16, $17, $18, $19)
    """, batch_buffer)
    batch_buffer.clear()
    print(f"📦 Run {run_id[:8]}... Batch #{batch_counter[run_id]} inserted {num_messages} messages")

async def score_after_delay(sub_id, run_id, pg_conn, redis_client, delay=5):
    """Wait `delay` seconds after last message, then compute final score."""
    await asyncio.sleep(delay)
    
    # if a newer message came in during sleep, this task is stale — abort
    if run_last_seen.get(run_id, 0) > asyncio.get_event_loop().time() - delay:
        return
    
    print(f"\n✅ Run {run_id} complete, computing final score...")
    new_score = await compute_scores(sub_id, run_id)
    await update_leaderboard(redis_client, new_score)
    print(f"🏆 Final score for {sub_id}: {new_score['score']}")
    
    # broadcast to React
    try:
        req = urllib.request.Request(
            'http://localhost:8001/broadcast',
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
    pg_conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5433/postgres")
    redis_client = await redis.from_url("redis://localhost:6379")
    
    consumer = AIOKafkaConsumer(
        "bot.metrics",
        bootstrap_servers="localhost:9092",
        group_id="telemetry-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest"
    )
    
    await consumer.start()
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
                await flush_batch(pg_conn, run_id)
            
            print(f"📥 Buffered order for run {run_id[:8]}... (buffer: {len(batch_buffer)})")
            
            # update last seen time for this run
            run_last_seen[run_id] = asyncio.get_event_loop().time()
            
            # cancel existing score task for this run if any
            if run_id in run_score_tasks:
                run_score_tasks[run_id].cancel()
            
            # schedule a new score task — will fire 5s after last message
            run_score_tasks[run_id] = asyncio.create_task(
                score_after_delay(sub_id, run_id, pg_conn, redis_client, delay=5)
            )
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # flush remaining messages - extract run_id from buffer if available
        if batch_buffer:
            run_id = batch_buffer[0][1]  # run_id is second element in tuple
            await flush_batch(pg_conn, run_id)
        await consumer.stop()
        await pg_conn.close()
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(consume_metrics())