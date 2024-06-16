package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
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
