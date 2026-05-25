package consumer

import (
	"context"
	"encoding/json"
	"log"

	"Ranabhum/bot-fleet/internal/bot"
	"github.com/segmentio/kafka-go"
)

const (
	TopicSubmissionReady = "submission.ready"
	GroupID              = "bot-fleet-workers"
)

// Handler is called for each new submission.ready event.
type Handler func(ctx context.Context, event bot.SubmissionReady) error

// SubmissionConsumer listens to the submission.ready Kafka topic.
type SubmissionConsumer struct {
	reader  *kafka.Reader
	handler Handler
}

// New creates a new SubmissionConsumer.
// brokers: e.g. []string{"redpanda:9092"}
func New(brokers []string, handler Handler) *SubmissionConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    TopicSubmissionReady,
		GroupID:  GroupID,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})
	return &SubmissionConsumer{reader: r, handler: handler}
}

// Run blocks and processes messages until ctx is cancelled.
func (c *SubmissionConsumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	log.Printf("[consumer] listening on topic=%s group=%s", TopicSubmissionReady, GroupID)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			log.Printf("[consumer] fetch error: %v", err)
			continue
		}

		var event bot.SubmissionReady
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[consumer] bad message, skipping: %v", err)
			c.reader.CommitMessages(ctx, msg)
			continue
		}

		log.Printf("[consumer] received submission_id=%s endpoint=%s:%d",
			event.SubmissionID, event.EndpointURL, event.Port)

		if err := c.handler(ctx, event); err != nil {
			log.Printf("[consumer] handler error for submission_id=%s: %v", event.SubmissionID, err)
			// Don't commit — retry on restart. Fine for hackathon.
			continue
		}

		c.reader.CommitMessages(ctx, msg)
	}
}
