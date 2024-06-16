package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 订阅模式创建rabbitmq实例
func NewRabbitMQPubSub(mqurl, exchangeName string) (*RabbitMQ, error) {
	//创建rabbitmq实例
	return NewRabbitMQ(mqurl, exchangeName, "", "")
}

// 订阅模式生成
func (r *RabbitMQ) PublishPub(message string) {
	//尝试创建交换机，不存在创建
	err := r.channel.ExchangeDeclare(
		//交换机名称
		r.Exchange,
		//交换机类型 广播类型
		"fanout",
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

	//发送消息
	err = r.channel.Publish(r.Exchange, "", false, false, amqp.Publishing{
		//类型
		ContentType: "text/plain",
		//消息
		Body: []byte(message),
	})
	if err != nil {
		fmt.Printf("Failed to publish a message:%v", err)
		return
	}
}

// 订阅模式消费端代码
func (r *RabbitMQ) RecieveSub() {

	ch := r.channel
	exchange := r.Exchange
	//尝试创建交换机，不存在创建
	err := ch.ExchangeDeclare(
		//交换机名称
		exchange,
		//交换机类型 广播类型
		"fanout",
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
	q, err := ch.QueueDeclare(
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
	err = ch.QueueBind(
		q.Name,
		//在pub/sub模式下，这里的key要为空
		"",
		exchange,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to bind a queue:%v", err)
		return
	}
	//消费消息
	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	//go func() {
	//	for {
	//		select {
	//		case data := <-msgs:
	//			//实现我们要处理的逻辑函数
	//			fmt.Printf("Received a message:%s\n", data.Body)
	//			//true表示回复当前信道所有未回复的ack，用于批量确认。false表示回复当前条目
	//			err = data.Ack(false)
	//			if err != nil {
	//				fmt.Printf("Failed to ack message:%v\n", err)
	//			}
	//		}
	//	}
	//}()
	var end chan struct{}
	go func() {
		for d := range msgs {
			fmt.Printf("Received a message:%s\n", d.Body)
			err = d.Ack(false)
			if err != nil {
				fmt.Printf("Failed to ack message:%v\n", err)
			}
		}
	}()
	<-end
}
