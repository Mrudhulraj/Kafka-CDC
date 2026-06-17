package consumer

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mrudhulraj/go-kafka/internal/shared"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	msgCH    chan<- string
}

func NewKafkaConsumer(msgch chan<- string) (*KafkaConsumer, error) {
	cfg := shared.NewkafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Host,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest", // From beginning
	})

	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{cfg.Topic}, nil)

	if err != nil {
		return nil, err
	}

	consumer := &KafkaConsumer{
		consumer: c,
		topic:    cfg.Topic,
		msgCH:    msgch,
	}
	go consumer.readMsgLoop()

	return consumer, nil
}

func (c *KafkaConsumer) readMsgLoop() {
	defer c.consumer.Close()
	// Infinite loop
	for {
		msg, err := c.consumer.ReadMessage(time.Second)
		if err != nil && !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		payload := msg.Value
		c.msgCH <- string(payload)
	}

}
