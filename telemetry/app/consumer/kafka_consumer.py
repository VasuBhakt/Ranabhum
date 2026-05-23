import json
import asyncio
import asyncpg
import urllib.request
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

from app.scoring.engine import compute_scores
from app.db.redis_store import update_leaderboard

async def consume_metrics():
    pg_conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5433/postgres")
    redis_client = await redis.from_url("redis://localhost:6379")
    
    consumer = AIOKafkaConsumer(
        "bot.metrics",
        bootstrap_servers="localhost:9092",
        group_id="telemetry-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    
    await consumer.start()
    print("⚡ Live Pipeline Started! Waiting for bot telemetry...")
    
    try:
        async for message in consumer:
            data = message.value
            sub_id = data["submission_id"]
            print(f"\n📥 Received order from {sub_id}...")
            
            await pg_conn.execute("""
                INSERT INTO order_metrics 
                (submission_id, run_id, order_id, order_type, 
                 sent_at, latency_ns, fill_correct, bot_id)
                VALUES ($1, $2, $3, $4, 
                        to_timestamp($5::double precision / 1e9), 
                        $6, $7, $8)
            """, 
            sub_id, data["run_id"], data["order_id"], 
            data["order_type"], data["sent_at_ns"], data["latency_ns"], 
            data["fill_correct"], data["bot_id"])
            
            new_score = await compute_scores(sub_id)
            await update_leaderboard(redis_client, new_score)
            print(f"🚀 Leaderboard updated! {sub_id} score is now {new_score['score']}")
            
            # --- NEW: Tell the FastAPI server to broadcast to React ---
            try:
                req = urllib.request.Request(
                    'http://localhost:8001/broadcast',
                    data=json.dumps(new_score).encode('utf-8'),
                    headers={'Content-Type': 'application/json'}
                )
                urllib.request.urlopen(req)
                print("📢 Broadcasted live to React!")
            except Exception as e:
                print(f"⚠️ Could not broadcast: {e}")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await consumer.stop()
        await pg_conn.close()
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(consume_metrics())