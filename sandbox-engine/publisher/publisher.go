package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sandbox-engine/model"

	"github.com/segmentio/kafka-go"
)

const TopicSubmissionReady = "submission.ready"

var writer *kafka.Writer

// Init initializes the global Kafka writer.
func Init(brokers []string) {
	writer = &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  TopicSubmissionReady,
		Balancer:               &kafka.Hash{}, // partition by submission_id for ordered delivery
		AllowAutoTopicCreation: true,
	}
	log.Printf("[publisher] Kafka writer initialized with brokers %v, topic=%s", brokers, TopicSubmissionReady)
}

// Close closes the Kafka writer.
func Close() error {
	if writer != nil {
		log.Println("[publisher] Closing Kafka writer...")
		return writer.Close()
	}
	return nil
}

// PublishSubmissionReady marshals and publishes a SubmissionReadyEvent to the submission.ready topic.
func PublishSubmissionReady(ctx context.Context, event model.SubmissionReadyEvent) error {
	if writer == nil {
		return fmt.Errorf("kafka writer is not initialized")
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal submission ready event: %w", err)
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.SubmissionID),
		Value: data,
	})
	if err != nil {
		log.Printf("[publisher] failed to publish message to topic %s: %v", TopicSubmissionReady, err)
		return err
	}

	log.Printf("[publisher] published submission.ready event for ID: %s", event.SubmissionID)
	return nil
}
