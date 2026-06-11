from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
import asyncpg
import json
import os

from app.db.redis_store import get_leaderboard

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

connected_clients = []

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

@app.get("/health")
async def health():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.fetchval("SELECT 1")
        await conn.close()
        return {"service": "telemetry", "database": "ok"}
    except Exception as e:
        return {"service": "telemetry", "database": str(e)}

@app.get("/leaderboard")
async def fetch_leaderboard():
    client = await redis.from_url(REDIS_URL)
    board = await get_leaderboard(client)
    await client.aclose()
    return board

# --- NEW: The Bridge ---
@app.post("/broadcast")
async def broadcast_score(request: Request):
    """Internal endpoint: The Kafka Consumer calls this to push new scores to React"""
    new_score = await request.json()
    message = json.dumps(new_score)
    
    dead_clients = []
    for client in connected_clients:
        try:
            await client.send_text(message)
        except:
            dead_clients.append(client)
            
    # Clean up disconnected browsers
    for client in dead_clients:
        connected_clients.remove(client)
        
    return {"status": "broadcasted"}

@app.websocket("/ws/leaderboard")
async def websocket_leaderboard(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)