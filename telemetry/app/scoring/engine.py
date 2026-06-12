import asyncpg
import asyncio
import os

async def compute_scores(submission_id: str, run_id: str, db_pool=None) -> dict:
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
        
        score = (0.4 * tps_val) + (0.4 * (1 / p99)) + (0.2 * correctness_val)

        return {
            "submission_id": submission_id,
            "run_id": run_id,
            "p50_ms": round(p50, 2),
            "p90_ms": round(p90, 2),
            "p99_ms": round(p99, 2),
            "tps": round(tps_val, 2),
            "correctness": round(correctness_val, 4),
            "score": round(score, 4)
        }
    finally:
        await release()

if __name__ == "__main__":
    print("Testing scoring engine...")
    result = asyncio.run(compute_scores("test-123", "run-123"))
    print("Score successfully computed:")
    print(result)