import { useEffect, useState } from "react"

interface Score {
  submission_id: string
  score: number
  p99_ms: number
  tps: number
  correctness: number
}

export default function App() {
  const [leaderboard, setLeaderboard] = useState<Score[]>([])

  useEffect(() => {
    // 1. Fetch the initial leaderboard data from your REST API
    fetch("http://localhost:8001/leaderboard")
      .then(r => r.json())
      .then(data => {
        const sorted = data.sort((a: Score, b: Score) => b.score - a.score)
        setLeaderboard(sorted)
      })

    // 2. Connect to the FastAPI WebSocket for live updates
    const ws = new WebSocket("ws://localhost:8001/ws/leaderboard")

    ws.onmessage = (event) => {
      const newScore: Score = JSON.parse(event.data)
      setLeaderboard(prev => {
        const updated = prev.filter(s => s.submission_id !== newScore.submission_id)
        return [...updated, newScore].sort((a, b) => b.score - a.score)
      })
    }

    return () => ws.close()
  }, [])

  return (
    <div style={{ padding: 32, fontFamily: "monospace", backgroundColor: "#1e1e1e", color: "#d4d4d4", minHeight: "100vh" }}>
      <h1 style={{ color: "#569cd6" }}>Live Hackathon Leaderboard</h1>
      
      <table style={{ width: "100%", textAlign: "left", borderCollapse: "collapse", marginTop: "20px" }}>
        <thead style={{ borderBottom: "1px solid #404040" }}>
          <tr>
            <th style={{ padding: "12px" }}>Rank</th>
            <th style={{ padding: "12px" }}>Submission</th>
            <th style={{ padding: "12px", color: "#ce9178" }}>Score</th>
            <th style={{ padding: "12px" }}>p99 (ms)</th>
            <th style={{ padding: "12px" }}>TPS</th>
            <th style={{ padding: "12px" }}>Correctness</th>
          </tr>
        </thead>
        <tbody>
          {leaderboard.length === 0 ? (
            <tr>
              <td colSpan={6} style={{ padding: "12px", textAlign: "center", color: "#808080" }}>
                Waiting for bot telemetry...
              </td>
            </tr>
          ) : (
            leaderboard.map((entry, i) => (
              <tr key={entry.submission_id} style={{ borderBottom: "1px solid #333" }}>
                <td style={{ padding: "12px" }}>#{i + 1}</td>
                <td style={{ padding: "12px", color: "#4ec9b0" }}>{entry.submission_id}</td>
                <td style={{ padding: "12px", color: "#ce9178", fontWeight: "bold" }}>{entry.score.toFixed(2)}</td>
                <td style={{ padding: "12px" }}>{entry.p99_ms}</td>
                <td style={{ padding: "12px" }}>{entry.tps}</td>
                <td style={{ padding: "12px" }}>{(entry.correctness * 100).toFixed(1)}%</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}