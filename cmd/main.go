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

func NewServer() *Server {
	msgCH := make(chan string, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	}

	return &Server{
		producer: producer.NewKafkaProducer(" "),
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

func (s *Server) handleMsg(msg string) {
	fmt.Printf("Received mesage: %s\n", msg)
}

func main() {
	srv := NewServer()
	go srv.produceMsg()

	for msg := range srv.msgCH {
		go srv.handleMsg(msg)
	}
}
