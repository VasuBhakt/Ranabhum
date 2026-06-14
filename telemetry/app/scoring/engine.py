import asyncpg
import asyncio
import os
import json

async def compute_scores(submission_id: str, run_id: str, db_pool=None, redis_client=None) -> dict:
    # Use pool if provided, otherwise fall back to direct connection (for standalone testing)
    if db_pool:
        conn = await db_pool.acquire()
        release = lambda: db_pool.release(conn)
    else:
        db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
        conn = await asyncpg.connect(db_url)
        release = conn.close
    
    try:
        # single query combines all stats
        result = await conn.fetchrow("""
            SELECT 
                percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ns) / 1e6 AS p50_ms,
                percentile_cont(0.90) WITHIN GROUP (ORDER BY latency_ns) / 1e6 AS p90_ms,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ns) / 1e6 AS p99_ms,
                COUNT(*) / GREATEST(
                    EXTRACT(EPOCH FROM (MAX(sent_at) - MIN(sent_at))), 1
                ) AS tps,
                AVG(fill_correct::int) AS correctness
            FROM order_metrics
            WHERE submission_id = $1 AND run_id = $2
        """, submission_id, run_id)

        p50 = float(result["p50_ms"] or 0)
        p90 = float(result["p90_ms"] or 0)
        p99 = float(result["p99_ms"] or 1)
        tps_val = float(result["tps"] or 0)
        correctness_val = float(result["correctness"] or 0)
        
        cert_score = -1.0
        team_name = "Unknown"
        if redis_client:
            run_data = await redis_client.get(f"run:{run_id}")
            if run_data:
                run_state = json.loads(run_data)
                cert_score = float(run_state.get("certification_score", -1.0))
                team_name = run_state.get("contestant_id", "Unknown")
        
        # 1. Normalize TPS (e.g. 10,000 TPS -> 100 points)
        normalized_tps = tps_val / 100.0
        
        # 2. Normalize Latency (p99). e.g., 1ms -> 50 points, 10ms -> 5 points
        normalized_latency = 50.0 / max(p99, 0.1)
        
        # Base performance is purely speed and responsiveness
        base_performance = normalized_tps + normalized_latency
        
        # 3. Correctness Multiplier
        # If correctness drops below 90%, it's considered broken and heavily penalized.
        # Otherwise, we use a power of 3 for a smooth, forgiving exponential decay.
        if correctness_val < 0.90:
            penalty = 0.01
        else:
            penalty = correctness_val ** 3
            
        score = base_performance * penalty
        
        # Certification bonus multiplier
        if cert_score >= 0.0:
            # 50% bonus for passing all certification passes
            score = score * (1.0 + (0.5 * cert_score))

        return {
            "submission_id": submission_id,
            "run_id": run_id,
            "team_name": team_name,
            "p50_ms": round(p50, 2),
            "p90_ms": round(p90, 2),
            "p99_ms": round(p99, 2),
            "tps": round(tps_val, 2),
            "correctness": round(correctness_val, 4),
            "cert_score": round(cert_score, 2),
            "score": round(score, 4)
        }
    finally:
        await release()

if __name__ == "__main__":
    print("Testing scoring engine...")
    result = asyncio.run(compute_scores("test-123", "run-123"))
    print("Score successfully computed:")
    print(result)