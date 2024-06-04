package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

func main() {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		log.Println("error in open the broker, ", err)
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
		log.Fatal("error in create new cluster ... ", err)
	}

	err = admin.CreateTopic("test-xuch001", &topicDetail, false)
	if err != nil {
		log.Println("error in create topic, ", err)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		log.Println("error in list topics, ", err)
	}
	for topicName, detail := range topics {
		log.Println("topic name: %v ,detail=%v\n", topicName, detail)
	}

	consumerGroups, err := admin.ListConsumerGroups()
	for consumerGroupName, consumerGroup := range consumerGroups {
		fmt.Println("consumerGroupName: %v ,consumerGroup=%v\n", consumerGroupName, consumerGroup)
	}

	err = admin.Close()
	if err != nil {
		log.Fatal("error in close admin, ", err)
	}

}
