package notifier

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/skroutz/downloader/job"
)

// KafkaBackend notifies about a job completion by producing to a Kafka topic.
type KafkaBackend struct {
	producer *kafka.Producer
}

func (b *KafkaBackend) ID() string {
	return "kafka"
}

func (b *KafkaBackend) Start() error {
	// TODO: initialize producer
	return nil
}

func (b *KafkaBackend) Notify(j *job.Job) error {
	return nil
}
