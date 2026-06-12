package publisher

import (
	"context"
	"encoding/json"
	"log"

	"Ranabhum/bot-fleet/internal/bot"
	"github.com/segmentio/kafka-go"
)

const TopicBotMetrics = "bot.metrics"

// MetricsPublisher sends BotMetrics events to the bot.metrics topic.
type MetricsPublisher struct {
	writer *kafka.Writer
}

func New(brokers []string) *MetricsPublisher {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  TopicBotMetrics,
		Balancer:               &kafka.Hash{}, // partition by submission_id for ordering
		AllowAutoTopicCreation: false,
		Async:                  true, // Enable asynchronous batch writes for low latency and high throughput
	}
	return &MetricsPublisher{writer: w}
}

// Publish sends a single BotMetrics event. Non-blocking — call from goroutines.
func (p *MetricsPublisher) Publish(ctx context.Context, m bot.BotMetrics) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(m.SubmissionID), // same partition for same submission
		Value: data,
	})
	if err != nil {
		log.Printf("[publisher] failed to write bot.metrics: %v", err)
	}
	return err
}

func (p *MetricsPublisher) Close() error {
	return p.writer.Close()
}
