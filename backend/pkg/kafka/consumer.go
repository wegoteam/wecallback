package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"time"
)

type kafkaComsumer struct {
	addrs    []string
	group    string
	config   *sarama.Config
	client   sarama.ConsumerGroup
	consumer sarama.Consumer
}

func NewKafkaComsumerGroup(addrs []string, group string, config *sarama.Config) (*kafkaComsumer, error) {

	if config == nil {
		config = sarama.NewConfig()
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
		//config.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.Consumer.Offsets.AutoCommit.Enable = false
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	}

	// 创建一个消费者组
	client, err := sarama.NewConsumerGroup(addrs, group, config)
	if err != nil {
		return nil, err
	}
	comsumer := &kafkaComsumer{
		addrs:  addrs,
		group:  group,
		config: config,
		client: client,
	}
	return comsumer, nil
}

func NewKafkaComsumer(addrs []string, group string, config *sarama.Config) (*kafkaComsumer, error) {

	if config == nil {
		config = sarama.NewConfig()
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
		//config.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.Consumer.Offsets.AutoCommit.Enable = false
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	}

	// 创建一个消费者组
	consumer, err := sarama.NewConsumer(addrs, config)
	if err != nil {
		return nil, err
	}
	kafkaComsumer := &kafkaComsumer{
		addrs:    addrs,
		group:    group,
		config:   config,
		consumer: consumer,
	}
	return kafkaComsumer, nil
}

func (c *kafkaComsumer) Close() {
	client := c.client
	comsumer := c.consumer
	if client != nil {
		client.Close()
	}

	if comsumer != nil {
		comsumer.Close()
	}
}
func (c *kafkaComsumer) Consume(topics []string) {
	client := c.client
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

func (c *kafkaComsumer) CustConsume(ctx context.Context, topics []string, consumerhHndler sarama.ConsumerGroupHandler) {
	client := c.client

	for {
		if err := client.Consume(ctx, topics, consumerhHndler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				fmt.Printf("Error from consumer: %v\n", err)
				return
			}
			fmt.Printf("Error from consumer: %v\n", err)
		}
	}
}

func (c *kafkaComsumer) ConsumePartition(topic string) {
	comsumer := c.consumer

	partitionlist, err := comsumer.Partitions(topic) //获取topic的所有分区
	if err != nil {
		fmt.Println("failed get partition list,err:", err)
		return
	}

	for partition := range partitionlist { // 遍历所有分区
		//根据消费者对象创建一个分区对象
		pc, err := comsumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Println("failed get partition consumer,err:", err)
			return
		}
		defer pc.Close()

		go func(consumer sarama.PartitionConsumer) {
			defer pc.AsyncClose() // 移除这行，因为已经在循环结束时关闭了
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v", msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(pc)
		time.Sleep(time.Second * 10)
	}
}

type KafkaGroupConsumer struct {
}

func (consumer *KafkaGroupConsumer) Setup(session sarama.ConsumerGroupSession) error {
	claims := session.Claims()

	fmt.Printf("Setup claims %v", claims)
	return nil
}

func (consumer *KafkaGroupConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	claims := session.Claims()
	fmt.Printf("Cleanup claims %v", claims)
	return nil
}

func (consumer *KafkaGroupConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic := claim.Topic()
	fmt.Printf("Consuming topic: %s\n", topic)
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				fmt.Printf("message channel was closed")
				return nil
			}

			fmt.Printf("Message claimed: topic=%s value = %s, timestamp = %v, topic = %s\n", topic, string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			session.Commit()
		case <-session.Context().Done():
			return nil
		}
	}
}
