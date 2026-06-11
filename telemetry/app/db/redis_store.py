import redis.asyncio as redis
import json
import asyncio

async def update_leaderboard(client, scores: dict):
    # Redis sorted set: automatically sorts everyone by their score
    await client.zadd(
        "leaderboard", 
        mapping={scores["submission_id"]: scores["score"]}
    )
    
    # Store the full score details (TPS, p99, etc.) so the UI can display them
    await client.set(
        f"scores:{scores['submission_id']}",
        json.dumps(scores),
        ex=3600 # Auto-deletes after 1 hour to save memory
    )

async def get_leaderboard(client) -> list:
    # Get the top 20 competitors, highest score first
    entries = await client.zrevrange("leaderboard", 0, 19, withscores=True)
    
    result = []
    for submission_id, score in entries:
        # Decode the ID from bytes to a string
        sub_id_str = submission_id.decode('utf-8') if isinstance(submission_id, bytes) else submission_id
        
        detail = await client.get(f"scores:{sub_id_str}")
        if detail:
            result.append(json.loads(detail))
            
    return result

if __name__ == "__main__":
    async def test_redis():
        print("Connecting to Redis...")
        import os
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        client = await redis.from_url(redis_url)
        
        # A dummy score to test our functions
        dummy_score = {
            "submission_id": "test-123",
            "p50_ms": 1.2,
            "p90_ms": 1.5,
            "p99_ms": 2.0,
            "tps": 500.0,
            "correctness": 1.0,
            "score": 85.5
        }
        
        print("Saving score to leaderboard...")
        await update_leaderboard(client, dummy_score)
        
        print("Fetching top 20 leaderboard...")
        board = await get_leaderboard(client)
        print("Current Leaderboard State:")
        print(json.dumps(board, indent=2))
        
        # Safely close the modern connection
        await client.aclose()

    asyncio.run(test_redis())