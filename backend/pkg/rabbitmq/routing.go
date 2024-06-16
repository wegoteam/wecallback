package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// 路由模式 创建RabbitMQ实例
func NewRabbitMQRouting(mqurl, exchagne string, routingKey string) (*RabbitMQ, error) {
	//创建rabbitmq实例
	return NewRabbitMQ(mqurl, "", exchagne, routingKey)
}

// 路由模式发送信息
func (r *RabbitMQ) PublishRouting(message string) {
	//尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.Exchange,
		//交换机类型 广播类型
		"direct",
		//是否持久化
		true,
		//是否字段删除
		false,
		//true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		//是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare an exchange:%v", err)
	}
	//发送信息
	err = r.channel.Publish(
		r.Exchange,
		//要设置
		r.RoutingKey,
		false,
		false,
		amqp.Publishing{
			//类型
			ContentType: "text/plain",
			//消息
			Body: []byte(message),
		})

	if err != nil {
		fmt.Printf("Failed to publish a message:%v", err)
	}
}

// 路由模式接收信息
func (r *RabbitMQ) RecieveRouting() {
	//尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.Exchange,
		//交换机类型 广播类型
		"direct",
		//是否持久化
		true,
		//是否字段删除
		false,
		//true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		//是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare an exchange:%v", err)
	}
	//创建队列，创建队列
	q, err := r.channel.QueueDeclare(
		"", //随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare a queue:%v", err)
	}
	//绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		//在pub/sub模式下，这里的key要为空
		r.RoutingKey,
		r.Exchange,
		false,
		nil,
	)
	//消费消息
	message, err := r.channel.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to register a consumer:%v", err)
	}
	forever := make(chan bool)
	go func() {
		for d := range message {
			log.Printf("Received a message:%s,", d.Body)
			err = d.Ack(false)
			if err != nil {
				fmt.Printf("Failed to ack message:%v\n", err)
			}
		}
	}()
	fmt.Println("退出请按 Ctrl+C")
	<-forever
}
