import asyncpg
import asyncio

async def compute_scores(submission_id: str) -> dict:
    # Connect using our working port 5433
    conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5433/postgres")
    
    try:
        latency = await conn.fetchrow("""
            SELECT 
                percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ns) / 1e6 AS p50_ms,
                percentile_cont(0.90) WITHIN GROUP (ORDER BY latency_ns) / 1e6 AS p90_ms,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ns) / 1e6 AS p99_ms
            FROM order_metrics 
            WHERE submission_id = $1
        """, submission_id)

        tps = await conn.fetchval("""
            SELECT COUNT(*) / GREATEST(
                EXTRACT(EPOCH FROM (MAX(sent_at) - MIN(sent_at))), 1
            )
            FROM order_metrics
            WHERE submission_id = $1
        """, submission_id)

        correctness = await conn.fetchval("""
            SELECT AVG(fill_correct::int)
            FROM order_metrics
            WHERE submission_id = $1
        """, submission_id)

        # --- THE FIX: Cast the Postgres Decimals to standard Python floats ---
        p99 = float(latency["p99_ms"] or 1)
        tps_val = float(tps or 0)
        correctness_val = float(correctness or 0)
        
        # Now the math will execute perfectly
        score = (0.4 * tps_val) + (0.4 * (1 / p99)) + (0.2 * correctness_val)

        return {
            "submission_id": submission_id,
            "p50_ms": round(float(latency["p50_ms"] or 0), 2),
            "p90_ms": round(float(latency["p90_ms"] or 0), 2),
            "p99_ms": round(p99, 2),
            "tps": round(tps_val, 2),
            "correctness": round(correctness_val, 4),
            "score": round(score, 4)
        }
    finally:
        await conn.close()

if __name__ == "__main__":
    print("Testing scoring engine...")
    result = asyncio.run(compute_scores("test-123"))
    print("Score successfully computed:")
    print(result)