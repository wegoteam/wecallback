package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"testing"
)

func TestBroker(t *testing.T) {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		fmt.Printf("error in open the broker, %v\n", err)
	}

	broker.Close()

	var topicDetail sarama.TopicDetail
	topicDetail.NumPartitions = 3
	topicDetail.ReplicationFactor = 1
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 //kafka版本号
	//config.Net.SASL.Enable = true
	//config.Net.SASL.Mechanism = "PLAIN"
	//config.Net.SASL.User = "admin"
	//config.Net.SASL.Password = "admin"

	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Printf("error in create new cluster ... %v\n", err)
	}

	err = admin.CreateTopic("test-xuch001", &topicDetail, false)
	if err != nil {
		fmt.Printf("error in create topic, %v\n", err)
	}

	err = admin.DeleteTopic("test-xuch001")
	if err != nil {
		fmt.Printf("error in delete topic, %v\n", err)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		fmt.Printf("error in list topics,%v\n ", err)
	}
	for topicName, detail := range topics {
		fmt.Printf("topic name: %v ,detail=%v\n", topicName, detail)
	}

	consumerGroups, err := admin.ListConsumerGroups()
	for consumerGroupName, consumerGroup := range consumerGroups {
		fmt.Printf("consumerGroupName: %v ,consumerGroup=%v\n", consumerGroupName, consumerGroup)
	}

	err = admin.Close()
	if err != nil {
		fmt.Printf("error in close admin,%v\n ", err)
	}

}
