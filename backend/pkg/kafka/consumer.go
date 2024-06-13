package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"strings"
)

type KafkaComsumerClient struct {
	brokers []string
	group   string
	topics  []string
	cofig   *sarama.Config
}

func NewConsumerGroupClient() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	brokers := "localhost:9092"
	group := "xuch-group"
	topics := []string{"xuch-topic"}
	// 创建一个消费者组
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	fmt.Printf("client: %v, err: %v\n", client, err)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := KafkaGroupConsumer{}
	for {
		if err := client.Consume(ctx, topics, &consumer); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				fmt.Printf("Error from consumer: %v\n", err)
				return
			}
			fmt.Printf("Error from consumer: %v\n", err)
		}
	}
}

func Consume() {

}

type KafkaGroupConsumer struct {
}

func (consumer *KafkaGroupConsumer) Setup(session sarama.ConsumerGroupSession) error {
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
