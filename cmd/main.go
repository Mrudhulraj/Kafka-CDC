package main

import (
	"fmt"
	"time"

	"github.com/mrudhulraj/go-kafka/internal/consumer"
	"github.com/mrudhulraj/go-kafka/internal/producer"
)

type Server struct {
	producer *producer.KafkaProducer
	consumer *consumer.KafkaConsumer
	msgCH    chan string
}

// Producer -> Consumer -> Message Channel (Has to be the DB here)
func NewServer() *Server {
	msgCH := make(chan string, 64)
	// Creates and reads from a topic
	// We are pushing data from producer to channel
	// Smart
	// Has go routines internally to read from any topic
	// This then collects the message and push the changes to the DB
	c, err := consumer.NewKafkaConsumerWithListening(msgCH)
	if err != nil {
		panic(err)
	}

	return &Server{
		// Just produces to a specific topic
		// Very dumb
		producer: producer.NewKafkaProducer("myTopic"),
		consumer: c,
		msgCH:    msgCH,
	}
}

func (s *Server) produceMsg() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	id := 0
	for t := range ticker.C {
		msg := fmt.Sprintf("Hello from Kafka = %d, ts= %s", id, t.Format("15:20:20"))
		id += 1
		s.producer.Produce(msg)
	}
}

// Note we havent handled  the consumer handling the producer from the channel
func (s *Server) handleMsg(msg string) {
	fmt.Printf("Received mesage: %s\n", msg)
}

func main() {
	srv := NewServer()
	// Topic not initialized yet

	// Produce messages now
	go srv.produceMsg()

	// Just keep printing whenever we get anything in the channel
	for msg := range srv.msgCH {
		go srv.handleMsg(msg)
	}
}
