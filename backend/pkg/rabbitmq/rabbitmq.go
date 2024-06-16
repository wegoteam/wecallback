package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	//连接
	conn *amqp.Connection
	//管道
	channel *amqp.Channel
	//队列名称
	QueueName string
	//交换机
	Exchange string
	//key Simple模式 几乎用不到
	RoutingKey string
	//连接信息
	Mqurl string
}

// NewRabbitMQ 创建RabbitMQ结构体实例
func NewRabbitMQ(mqurl, queueName, exchange, routingKey string) (*RabbitMQ, error) {
	rabbitmq := &RabbitMQ{QueueName: queueName, Exchange: exchange, RoutingKey: routingKey, Mqurl: mqurl}
	var err error
	//创建rabbitmq连接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	if err != nil {
		return nil, err
	}
	//创建channel
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	if err != nil {
		return nil, err
	}
	return rabbitmq, nil
}

// Close 断开channel和connection
func (r *RabbitMQ) Close() {
	err := r.channel.Close()
	if err != nil {
		fmt.Printf("Failed to close channel:%v", err)
	}
	err = r.conn.Close()
	if err != nil {
		fmt.Printf("Failed to close connection:%v", err)
	}
}
