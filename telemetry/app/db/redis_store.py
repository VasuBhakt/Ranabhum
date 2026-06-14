import redis.asyncio as redis
import json
import asyncio

async def update_leaderboard(client, scores: dict):
    team_name = scores.get("team_name", "Unknown")
    final_score = scores["score"]
    
    # Get the team's current all-time best score
    current_score = await client.zscore("leaderboard", team_name)
    
    # Only update the leaderboard if this is their first submission, 
    # or if this new submission beat their previous best score!
    if current_score is None or final_score > current_score:
        # Redis sorted set: automatically sorts everyone by their score
        await client.zadd(
            "leaderboard", 
            mapping={team_name: final_score}
        )
        
        # Store the full score details (TPS, p99, etc.) keyed by team_name
        await client.set(
            f"scores:{team_name}",
            json.dumps(scores)
        )

async def get_leaderboard(client) -> list:
    # Get the top 20 competitors, highest score first
    entries = await client.zrevrange("leaderboard", 0, 19, withscores=True)
    
    result = []
    for team_name, score in entries:
        # Decode the team name from bytes to a string
        team_name_str = team_name.decode('utf-8') if isinstance(team_name, bytes) else team_name
        
        detail = await client.get(f"scores:{team_name_str}")
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