package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/skroutz/downloader/job"
)

var eventsWG sync.WaitGroup

// KafkaBackend notifies about a job completion by producing to a Kafka topic.
type KafkaBackend struct {
	producer *kafka.Producer
	reports  chan job.CallbackInfo
}

// ID returns the identifier code for the respective backend
// which is "kafka".
func (b *KafkaBackend) ID() string {
	return "kafka"
}

// Start sets up a kafka producer and a channel for delivery reports.
func (b *KafkaBackend) Start(ctx context.Context, cfg map[string]interface{}) error {
	var (
		err         error
		kafkaConfig kafka.ConfigMap
	)

	kafkaConfig = make(kafka.ConfigMap)

	for key, value := range cfg {
		kafkaConfig.SetKey(key, value)
	}

	b.producer, err = kafka.NewProducer(&kafkaConfig)
	if err != nil {
		return err
	}

	b.reports = make(chan job.CallbackInfo)

	// start a go routine to monitor Kafka's Events channel
	go b.transformStream(ctx, &eventsWG)

	return nil
}

// Notify produces (asynchronously) a payload message into a kafka topic using
// the producer. Kafka will send the delivery report to the default Events delivery channel.
func (b *KafkaBackend) Notify(destination string, payload []byte) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &destination, Partition: kafka.PartitionAny},
		Value:          payload,
	}
	err := b.producer.Produce(message, nil)
	return err
}

// DeliveryReports returns a channel of emmited callback info events
func (b *KafkaBackend) DeliveryReports() <-chan job.CallbackInfo {
	return b.reports
}

// Finalize closes the reports channel and the producer.
// It also handles gracefully the termination of any go routines
// that the kafka backend started.
func (b *KafkaBackend) Finalize() error {
	close(b.reports)
	eventsWG.Wait()
	b.producer.Close()
	return nil
}

// transformStream iterates over the Events channel of Kafka, transforms
// each message to a callback info object and enqueues the callback info object
// to our reports channel.
func (b *KafkaBackend) transformStream(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context has been cancelled")
			return
		case e, ok := <-b.producer.Events():
			if !ok {
				fmt.Println("Kafka Events channel has been closed!")
				return
			}

			switch ev := e.(type) {
			case *kafka.Message:
				var cbInfo job.CallbackInfo
				json.Unmarshal(ev.Value, &cbInfo)

				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					cbInfo.Error = ev.TopicPartition.Error.Error()
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}

				b.reports <- cbInfo
			}
			continue
		}
	}
}
