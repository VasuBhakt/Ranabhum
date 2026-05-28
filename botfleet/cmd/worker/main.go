package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"Ranabhum/bot-fleet/internal/bot"
	botrunner "Ranabhum/bot-fleet/internal/bot"
	"Ranabhum/bot-fleet/internal/publisher"
	util "Ranabhum/bot-fleet/internal/util"
)

// This binary is used when spawning bots as individual Kubernetes Jobs.
// Env vars are injected by the coordinator or k8s manifest.
func main() {
	botID := util.MustEnv("BOT_ID")
	runID := util.MustEnv("RUN_ID")
	submissionID := util.MustEnv("SUBMISSION_ID")
	targetURL := util.MustEnv("TARGET_URL")
	brokers := util.MustEnv("KAFKA_BROKERS")
	ratePerSec := util.IntEnv("RATE_PER_SEC", 50)
	duration := util.DurationEnv("RUN_DURATION", 60*time.Second)

	pub := publisher.New([]string{brokers})
	defer pub.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := botrunner.Config{
		BotID:        botID,
		RunID:        runID,
		SubmissionID: submissionID,
		TargetURL:    targetURL,
		Duration:     duration,
		RatePerSec:   ratePerSec,
	}

	log.Printf("[worker] starting bot_id=%s run_id=%s target=%s rate=%d/s duration=%s",
		botID, runID, targetURL, ratePerSec, duration)

	err := botrunner.Run(ctx, cfg, func(m bot.BotMetrics) {
		pub.Publish(ctx, m)
	})
	if err != nil && ctx.Err() == nil {
		log.Fatalf("[worker] exited with error: %v", err)
	}
	log.Printf("[worker] bot_id=%s done", botID)
}
