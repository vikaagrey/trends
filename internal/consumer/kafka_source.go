package consumer

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

type KafkaSource struct {
	reader *kafka.Reader
}

type KafkaConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
}

func NewKafkaSource(config KafkaConfig) *KafkaSource {
	minBytes := config.MinBytes
	if minBytes <= 0 {
		minBytes = 1
	}
	maxBytes := config.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  config.Brokers,
		Topic:    config.Topic,
		GroupID:  config.GroupID,
		MinBytes: minBytes,
		MaxBytes: maxBytes,
	})
	return &KafkaSource{reader: reader}
}

func (source *KafkaSource) Fetch(ctx context.Context) (Message, error) {
	kafkaMessage, err := source.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("fetch from Kafka: %w", err)
	}
	return NewMessage(kafkaMessage.Value, kafkaMessage), nil
}

func (source *KafkaSource) Commit(ctx context.Context, message Message) error {
	kafkaMessage, ok := message.Handle().(kafka.Message)
	if !ok {
		return nil
	}
	if err := source.reader.CommitMessages(ctx, kafkaMessage); err != nil {
		return fmt.Errorf("commit to Kafka: %w", err)
	}
	return nil
}

func (source *KafkaSource) Close() error {
	if err := source.reader.Close(); err != nil {
		return fmt.Errorf("close Kafka reader: %w", err)
	}
	return nil
}
