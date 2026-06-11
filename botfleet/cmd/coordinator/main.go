package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"Ranabhum/bot-fleet/internal/bot"
	"Ranabhum/bot-fleet/internal/consumer"
	"Ranabhum/bot-fleet/internal/publisher"
	"Ranabhum/bot-fleet/internal/state"
	util "Ranabhum/bot-fleet/internal/util"

	"github.com/google/uuid"
)

func main() {
	brokers := strings.Split(util.MustEnv("KAFKA_BROKERS"), ",") // e.g. redpanda:9092
	redisAddr := util.MustEnv("REDIS_ADDR")                      // e.g. redis:6379
	botCount := util.IntEnv("BOT_COUNT", 10)                     // bots per submission
	ratePerBot := util.IntEnv("RATE_PER_BOT", 50)                // orders/sec per bot
	runDuration := util.DurationEnv("RUN_DURATION", 60*time.Second)

	store := state.New(redisAddr)
	pub := publisher.New(brokers)
	defer pub.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Ping(ctx); err != nil {
		log.Fatalf("[coordinator] redis not reachable: %v", err)
	}
	log.Printf("[coordinator] connected to redis=%s kafka=%v", redisAddr, brokers)
	log.Printf("[coordinator] config: bots=%d rate=%d/s duration=%s", botCount, ratePerBot, runDuration)

	// Handler called when a new submission arrives.
	handler := func(ctx context.Context, event bot.SubmissionReady) error {
		runID := uuid.NewString()
		var targetURL string
		if event.Port > 0 {
			targetURL = fmt.Sprintf("%s:%d", event.EndpointURL, event.Port)
		} else {
			targetURL = event.EndpointURL
		}

		run := bot.RunState{
			RunID:        runID,
			SubmissionID: event.SubmissionID,
			Status:       "RUNNING",
			BotCount:     botCount,
			StartedAt:    time.Now().UnixNano(),
		}
		if err := store.SetRun(ctx, run); err != nil {
			return fmt.Errorf("failed to persist run state: %w", err)
		}

		log.Printf("[coordinator] starting run_id=%s submission_id=%s target=%s bots=%d",
			runID, event.SubmissionID, targetURL, botCount)

		// Spin up N bots concurrently.
		var wg sync.WaitGroup
		for i := 0; i < botCount; i++ {
			wg.Add(1)
			go func(botIdx int) {
				defer wg.Done()
				cfg := bot.Config{
					BotID:        fmt.Sprintf("%s-bot-%d", runID, botIdx),
					RunID:        runID,
					SubmissionID: event.SubmissionID,
					TargetURL:    targetURL,
					Duration:     runDuration,
					RatePerSec:   ratePerBot,
				}
				err := bot.Run(ctx, cfg, func(m bot.BotMetrics) {
					pub.Publish(ctx, m) // fire-and-forget
				})
				if err != nil && ctx.Err() == nil {
					log.Printf("[bot %s] exited with error: %v", cfg.BotID, err)
				}
			}(i)
		}

		// Wait in background so we don't block the consumer.
		go func() {
			wg.Wait()
			
			sandboxEngineURL := os.Getenv("SANDBOX_ENGINE_URL")
			if sandboxEngineURL == "" {
				sandboxEngineURL = "http://localhost:8080"
			}
			url := fmt.Sprintf("%s/submissions/%s", sandboxEngineURL, event.SubmissionID)
			log.Printf("[coordinator] run_id=%s cleaning up container for submission_id=%s via %s", runID, event.SubmissionID, url)
			
			req, err := http.NewRequest("DELETE", url, nil)
			if err == nil {
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					resp.Body.Close()
					log.Printf("[coordinator] run_id=%s container cleanup request sent successfully", runID)
				} else {
					log.Printf("[coordinator] run_id=%s container cleanup failed: %v", runID, err)
				}
			}

			finalStatus := "DONE"
			if ctx.Err() != nil {
				finalStatus = "FAILED"
			}
			store.UpdateStatus(ctx, runID, finalStatus)
			log.Printf("[coordinator] run_id=%s finished status=%s", runID, finalStatus)
		}()

		return nil
	}

	c := consumer.New(brokers, handler)
	log.Println("[coordinator] bot-fleet ready, waiting for submissions...")
	if err := c.Run(ctx); err != nil {
		log.Printf("[coordinator] consumer exited: %v", err)
	}
}
