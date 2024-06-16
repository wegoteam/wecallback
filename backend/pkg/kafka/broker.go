package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
)

// kafkaBroker
// @Description:
type kafkaBroker struct {
	addrs        []string //kafka broker地址
	config       sarama.Config
	clusterAdmin sarama.ClusterAdmin
	brokerMap    map[string]*sarama.Broker
}

func NewKafkaAdmin(addrs []string, config *sarama.Config) (*kafkaBroker, error) {

	if config == nil {
		config = sarama.NewConfig()
		config.Version = sarama.V2_1_0_0 //kafka版本号
		//config.Net.SASL.Enable = true
		//config.Net.SASL.Mechanism = "PLAIN"
		//config.Net.SASL.User = "admin"
		//config.Net.SASL.Password = "admin"
	}
	brokerMap := make(map[string]*sarama.Broker)
	for _, addr := range addrs {
		broker := sarama.NewBroker(addr)
		brokerMap[addr] = broker
	}
	//broker := sarama.NewBroker("localhost:9092")
	//
	//err := broker.Open(config)
	//if err != nil {
	//	fmt.Printf("error in open the broker, %v\n", err)
	//}

	clusterAdmin, err := sarama.NewClusterAdmin(addrs, config)
	if err != nil {
		fmt.Printf("error in create new cluster ... %v\n", err)
		return nil, err
	}
	kafkaBroker := &kafkaBroker{
		addrs:        addrs,
		config:       *config,
		brokerMap:    brokerMap,
		clusterAdmin: clusterAdmin,
	}

	return kafkaBroker, nil
}

func (b *kafkaBroker) Close() {
	if b.clusterAdmin != nil {
		err := b.clusterAdmin.Close()
		if err != nil {
			fmt.Printf("error in close the cluster admin, %v\n", err)
		}
	}

	for _, broker := range b.brokerMap {
		err := broker.Close()
		if err != nil {
			fmt.Printf("error in close the broker, %v\n", err)
		}
	}
}

func (b *kafkaBroker) ListTopics() {
	clusterAdmin := b.clusterAdmin

	topics, err := clusterAdmin.ListTopics()
	if err != nil {
		fmt.Printf("error in list topics, %v\n", err)
	}
	for _, topic := range topics {
		fmt.Printf("topic: %v\n", topic)
	}
}

func (b *kafkaBroker) CreateTopic(topic string, partitions, replication int64) {
	clusterAdmin := b.clusterAdmin

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replication),
	}

	err := clusterAdmin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		fmt.Printf("error in create topic, %v\n", err)
	}
}

func (b *kafkaBroker) DeleteTopic(topic string) {
	clusterAdmin := b.clusterAdmin

	err := clusterAdmin.DeleteTopic(topic)
	if err != nil {
		fmt.Printf("error in delete topic, %v\n", err)
	}
}

func (b *kafkaBroker) ListConsumerGroups() {
	clusterAdmin := b.clusterAdmin

	consumerGroups, err := clusterAdmin.ListConsumerGroups()
	if err != nil {
		fmt.Printf("error in list consumer groups, %v\n", err)
	}
	for _, consumerGroup := range consumerGroups {
		fmt.Printf("consumer group: %v\n", consumerGroup)
	}
}
