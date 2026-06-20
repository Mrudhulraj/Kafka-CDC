package producer

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	shared "github.com/mrudhulraj/go-kafka/internal/shared"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(topic string) *KafkaProducer {
	// Get the base config which is basically
	// Topic, Host and the ConsumerGroup
	cfg := shared.NewkafkaConfig()

	// Create the Kafka Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		panic(err)
	}

	// defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() { // Iterate through a all the events and assign the to the right consumer group
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition) // Deliver to the right partition
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	newProducer := &KafkaProducer{
		producer: p,
		topic:    topic,
	}
	return newProducer
}

func (p *KafkaProducer) Produce(word string) {
	// Produce messages to topic (asynchronously)
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          []byte(word),
	}, nil)

	if err != nil {
		log.Printf("Error: %v", err)
	}

}
