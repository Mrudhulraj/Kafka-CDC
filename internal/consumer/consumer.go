package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mrudhulraj/go-kafka/internal/shared"
	"github.com/sirupsen/logrus"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	msgCH    chan<- *shared.Message
	readyCH  chan struct{}
	exitCH   chan struct{}
	isReady  bool

	msgStateMap  map[kafka.Offset]bool
	lastComitted kafka.Offset
	maxReceived  *kafka.TopicPartition
	mu           *sync.RWMutex
	commitDur    time.Duration
}

func NewKafkaConsumerWithListening(msgch chan<- string) (*KafkaConsumer, error) {
	cfg := shared.NewkafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Host,
		"group.id":           cfg.ConsumerGroup,
		"enable.auto.commit": false,
	})

	if err != nil {
		return nil, err
	}
	// Create a partition
	tp := kafka.TopicPartition{
		Topic:     &cfg.Topic,
		Partition: 0,
	}

	//Commit to a partition
	commited, err := c.Committed([]kafka.TopicPartition{tp}, int(time.Second)*5)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	// Get the latest commit
	latestCommit := commited[len(commited)-1].Offset
	logrus.WithField("OFFSET", latestCommit)

	// Track last Offset in a variable
	maxReceived := &kafka.TopicPartition{
		Topic:     tp.Topic,
		Partition: tp.Partition,
		Offset:    latestCommit,
	}

	consumer := &KafkaConsumer{
		consumer: c,
		msgCH:    make(chan *shared.Message, 64),
		readyCH:  make(chan struct{}),
		exitCH:   make(chan struct{}),
		isReady:  false,
		topic:    cfg.Topic,

		mu:           new(sync.RWMutex),
		msgStateMap:  map[kafka.Offset]bool{},
		lastComitted: latestCommit,
		maxReceived:  maxReceived,
		commitDur:    5 * time.Second,
	}

	consumer.initializeKafkaTopic(cfg.Host, cfg.Topic)
	err = c.Assign([]kafka.TopicPartition{
		{
			Topic:     &cfg.Topic,
			Partition: tp.Partition,
			Offset:    latestCommit,
		},
	})
	err = c.SubscribeTopics([]string{cfg.Topic}, nil)

	if err != nil {
		return nil, err
	}
	go consumer.checkReadyToAccept()
	go consumer.readMsgLoop()

	return consumer, nil
}

func (c *KafkaConsumer) readMsgLoop() {
	defer c.consumer.Close()
	// Infinite loop
	// Have to check if the topic is ready or not before reading
	for {
		msg, err := c.consumer.ReadMessage(time.Second)
		// CASE: No messages to read, within specific time so move to next message
		if err != nil && err.(kafka.Error).IsTimeout() {
			continue
		}
		// CASE: Report actual error if failed
		if err != nil && !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}
		// Track the message state in the partition
		c.appendMessageState(&msg.TopicPartition)
		c.msgCH <- shared.NewMessage(&msg.TopicPartition, msg.Value)
	}
}

func (c *KafkaConsumer) appendMessageState(tp *kafka.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Set the state to false since we have not begun processing
	c.msgStateMap[tp.Offset] = false
	if c.maxReceived.Offset < tp.Offset {
		c.maxReceived = &kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    tp.Offset,
		}
	}

}

func (c *KafkaConsumer) initializeKafkaTopic(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	log.Printf("Creating topic: %s", topicName)

	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			logrus.Infof("Topic already exists: %v", result.Error)
			continue
		}
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to create topic: %v", result.Topic)
		}
		log.Printf("Topic %s succesfully created", result.Topic)
	}

	return c.waitForTopicReady(brokers, topicName)
}

func (c *KafkaConsumer) waitForTopicReady(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})

	if err != nil {
		return err
	}
	defer adminClient.Close()

	for {
		time.Sleep(1 * time.Second)
		metadata, err := adminClient.GetMetadata(&topicName, false, 5000)

		if err != nil {
			logrus.Errorf("Metadata fetch failed %v \n", err)
			continue
		}

		topicMeta, exists := metadata.Topics[topicName]
		if !exists {
			continue
		}

		if len(topicMeta.Partitions) > 0 {
			allPartitionisReady := true
			for _, partition := range topicMeta.Partitions {
				if partition.Error.Code() != kafka.ErrNoError {
					allPartitionisReady = false
					break
				}
				if partition.Leader == -1 {
					allPartitionisReady = false
					break
				}
			}

			logrus.WithField("IS_INTIIALIZED", allPartitionisReady).Info("Consumer Topic")

			if allPartitionisReady {
				return nil
			}
		}

	}
}

func (c *KafkaConsumer) checkReadyToAccept() error {
	defer func() {
		c.isReady = true
	}()
	for {
		select {
		case <-c.readyCH:
			return nil
		default:
			time.Sleep(1 * time.Second)
			isReady, err := c.readyCheck()
			if err != nil {
				logrus.Error("Error on consumer readycheck")
				return err
			}
			logrus.WithField("STATUS", isReady).Warn("Consumer ready to accept")

			if isReady {
				return nil
			}
		}

	}
}

func (c *KafkaConsumer) readyCheck() (bool, error) {
	assignment, err := c.consumer.Assignment()
	if err != nil {
		logrus.Errorf("Failed to get assignment: %v", err)
		return false, err
	}

	return len(assignment) > 0, nil
}
