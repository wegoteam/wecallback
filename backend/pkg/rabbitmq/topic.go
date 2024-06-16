package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// 话题模式 创建RabbitMQ实例
func NewRabbitMQTopic(mqurl, exchagne string, routingKey string) (*RabbitMQ, error) {
	return NewRabbitMQ(mqurl, "", exchagne, routingKey)
}

// 话题模式发送信息
func (r *RabbitMQ) PublishTopic(message string) {
	//尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.Exchange,
		//交换机类型 话题模式
		"topic",
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
		return
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

// 话题模式接收信息
// 要注意key
// 其中* 用于匹配一个单词，#用于匹配多个单词（可以是零个）
// 匹配 表示匹配imooc.* 表示匹配imooc.hello,但是imooc.hello.one需要用imooc.#才能匹配到
func (r *RabbitMQ) RecieveTopic() {
	//尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.Exchange,
		//交换机类型 话题模式
		"topic",
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
		return
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
		return
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

	if err != nil {
		fmt.Printf("Failed to bind a queue:%v", err)
		return
	}
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
		return
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
