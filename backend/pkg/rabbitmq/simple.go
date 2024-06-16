package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

// NewRabbitMQSimple 简单模式step：1。创建简单模式下RabbitMQ实例
func NewRabbitMQSimple(mqurl, queueName string) (*RabbitMQ, error) {
	return NewRabbitMQ(mqurl, queueName, "", "")
}

// 简单模式Step:2、简单模式下生产代码
func (r *RabbitMQ) PublishSimple(message string) {
	//1、申请队列，如果队列存在就跳过，不存在创建
	//优点：保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		//队列名称
		r.QueueName,
		//是否持久化
		false,
		//是否为自动删除 当最后一个消费者断开连接之后，是否把消息从队列中删除
		false,
		//是否具有排他性 true表示自己可见 其他用户不能访问
		false,
		//是否阻塞 true表示要等待服务器的响应
		false,
		//额外数学系
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to declare a queue:%v", err)
		return
	}

	//2.发送消息到队列中
	err = r.channel.Publish(
		//默认的Exchange交换机是default,类型是direct直接类型
		r.Exchange,
		//要赋值的队列名称
		r.QueueName,
		//如果为true，根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返回给发送者
		false,
		//如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者，则会把消息还给发送者
		false,
		//消息
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

func (r *RabbitMQ) ConsumeSimple() {
	//1、申请队列，如果队列存在就跳过，不存在创建
	//优点：保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		//队列名称
		r.QueueName,
		//是否持久化
		false,
		//是否为自动删除 当最后一个消费者断开连接之后，是否把消息从队列中删除
		false,
		//是否具有排他性
		false,
		//是否阻塞
		false,
		//额外数学系
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	//接收消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		//用来区分多个消费者
		"",
		//是否自动应答
		false,
		//是否具有排他性
		false,
		//如果设置为true,表示不能同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//队列是否阻塞
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to register a consumer:%v", err)
		return
	}
	//ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancelFunc()
	//go func() {
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			return
	//		case data := <-msgs:
	//			//实现我们要处理的逻辑函数
	//			fmt.Printf("Received a message:%s\n", data.Body)
	//			err = data.Ack(true)
	//			if err != nil {
	//				fmt.Printf("Failed to ack message:%v\n", err)
	//			}
	//		}
	//	}
	//}()
	//time.Sleep(time.Second * 11)

	var end chan struct{}
	for d := range msgs {
		//实现我们要处理的逻辑函数
		fmt.Printf("Received a message:%s\n", d.Body)
		err = d.Ack(true)
		if err != nil {
			fmt.Printf("Failed to ack message:%v\n", err)
		}
	}
	<-end
}
