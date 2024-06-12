package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
)

type KafkaGroupClient struct {
	brokers []string
	group   string
	topics  []string
	cofig   *sarama.Config
}

type KafkaGroupConsumer struct {
	ready chan bool
}

func (consumer *KafkaGroupConsumer) Setup(session sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *KafkaGroupConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *KafkaGroupConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				fmt.Printf("message channel was closed")
				return nil
			}
			fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			session.Commit()
		case <-session.Context().Done():
			return nil
		}
	}
}
