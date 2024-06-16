package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"time"
)

type kafkaProducer struct {
	addrs  []string
	cofig  *sarama.Config
	client sarama.SyncProducer
}

func NewKafkaProducer(addrs []string, config *sarama.Config) (*kafkaProducer, error) {
	if config == nil {
		// 创建一个配置对象
		config = sarama.NewConfig()
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
	}

	// 使用broker地址和配置创建一个同步Producer
	client, err := sarama.NewSyncProducer(addrs, config)
	producer := &kafkaProducer{
		addrs:  addrs,
		cofig:  config,
		client: client,
	}
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		return nil, err
	}
	return producer, err
}

func (p *kafkaProducer) SendMessage(topic, data string) {
	producer := p.client
	// 要发送的消息
	messageDetail := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(data),
	}

	// 发送消息
	dataPart, dataOffset, err := producer.SendMessage(messageDetail)
	if err != nil {
		fmt.Printf("Failed to send message: %v", err)
	}

	// 打印消息发送详情
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", messageDetail.Topic, dataPart, dataOffset)
}

func (p *kafkaProducer) SendKeyMessage(topic, key, data string) {
	producer := p.client
	// 要发送的消息
	messageDetail := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(data),
	}

	// 发送消息
	dataPart, dataOffset, err := producer.SendMessage(messageDetail)
	if err != nil {
		fmt.Printf("Failed to send message: %v", err)
	}

	// 打印消息发送详情
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", messageDetail.Topic, dataPart, dataOffset)
}

func (p *kafkaProducer) SendDetailMessage(topic, key, data string, partition, offset int64, timestamp time.Time) {
	producer := p.client
	// 要发送的消息
	messageDetail := &sarama.ProducerMessage{
		Topic:     topic,
		Timestamp: timestamp,
		Partition: int32(partition),
		Offset:    offset,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(data),
	}

	// 发送消息
	dataPart, dataOffset, err := producer.SendMessage(messageDetail)
	if err != nil {
		fmt.Printf("Failed to send message: %v", err)
	}

	// 打印消息发送详情
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", messageDetail.Topic, dataPart, dataOffset)
}

func (p *kafkaProducer) Close() {
	err := p.client.Close()
	if err != nil {
		fmt.Printf("Failed to close producer: %v\n", err)
	}
}
