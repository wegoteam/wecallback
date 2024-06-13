package kafka

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
)

func TestProducer(t *testing.T) {
	// Kafka集群的broker地址列表
	brokerList := []string{"localhost:9092"}

	// 创建一个配置对象
	config := sarama.NewConfig()
	// 设置Producer所需的确认模式，这里设置为等待所有同步副本确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 设置分区器，这里使用随机分区器
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//config.Producer.Partitioner = sarama.NewManualPartitioner
	//config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	//config.Producer.Partitioner = sarama.NewHashPartitioner
	//config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	// 设置消息成功发送时返回
	config.Producer.Return.Successes = true

	// 使用broker地址和配置创建一个同步Producer
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Printf("Failed to close producer: %v", err)
		}
	}()

	// 要发送的消息
	message := &sarama.ProducerMessage{
		Topic: "test_xuch2",
		Key:   sarama.StringEncoder("test_key"),
		Value: sarama.StringEncoder("test message"),
	}

	// 发送消息
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Printf("Failed to send message: %v", err)
	}

	// 打印消息发送详情
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", message.Topic, partition, offset)
}
