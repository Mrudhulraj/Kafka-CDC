package shared

type kafkaConfig struct {
	Topic         string
	ConsumerGroup string
	Host          string
}

func NewkafkaConfig() *kafkaConfig {
	return &kafkaConfig{
		Topic:         "myTopic",
		ConsumerGroup: "myGroup",
		Host:          "localhost",
	}

}
