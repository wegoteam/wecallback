package kafka

import (
	"github.com/IBM/sarama"
)

type kafkaBroker struct {
	addrs     []string //kafka broker地址
	isCluster bool     //是否是集群
}
type kafkaConfig struct {
	sarama.Config
}

func NewKafkaAdmin() {

}
