package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
	"testing"
)

func TestRabbitMQSimple(t *testing.T) {
	//Simple模式
	rabbitMQSimple, err := NewRabbitMQSimple("simpleQueue")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	//发送者
	rabbitMQSimple.PublishSimple("message hello world")
	rabbitMQSimple.Destory()
}

func TestRabbitMQSimpleConsume(t *testing.T) {
	//Simple模式
	rabbitMQSimple, err := NewRabbitMQSimple("simpleQueue")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	//接收者
	rabbitMQSimple.ConsumeSimple()
	rabbitMQSimple.Destory()
}

func TestRabbitMQPub(t *testing.T) {
	//订阅模式发送者
	rabbitMQPubSub, err := NewRabbitMQPubSub("pubSubEx")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	for i := 0; i < 100; i++ {
		rabbitMQPubSub.PublishPub(fmt.Sprintf("pubsub message data %d", i))
	}

	//销毁
	rabbitMQPubSub.Destory()
}

func TestRabbitMQSub(t *testing.T) {
	//接收者
	rabbitMQPubSubConsume, err := NewRabbitMQPubSub("pubSubEx")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	rabbitMQPubSubConsume.RecieveSub()

	//销毁
	rabbitMQPubSubConsume.Destory()
}

func TestRabbitMQPubSub1(t *testing.T) {
	conn, err := amqp.Dial("amqp://rabbit:rabbit@localhost:5672/")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel:%v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"pubSubEx", // name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		fmt.Printf("Failed to declare an exchange:%v", err)
	}

	err = ch.Publish("pubSubEx", // exchange
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello world111"),
		})
	if err != nil {
		fmt.Printf("Failed to publish a message:%v", err)
	}
}

func TestRabbitMQPubSub2(t *testing.T) {
	conn, err := amqp.Dial("amqp://rabbit:rabbit@localhost:5672/")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel:%v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"pubSubEx", // name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		fmt.Printf("Failed to declare an exchange:%v", err)
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Failed to declare a queue:%v", err)
	}

	err = ch.QueueBind(
		q.Name,     // queue name
		"",         // routing key
		"pubSubEx", // exchange
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to bind a queue:%v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		fmt.Printf("Failed to register a consumer:%v", err)
	}

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func TestRabbitMQRouting(t *testing.T) {
	//路由模式发送者
	rabbitMQRoutingOne, err := NewRabbitMQRouting("routerEx", "router_one")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	rabbitMQRoutingTwo, err := NewRabbitMQRouting("routerEx", "router_two")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}

	for i := 0; i <= 10; i++ {
		rabbitMQRoutingOne.PublishRouting("router message one" + strconv.Itoa(i))
		rabbitMQRoutingTwo.PublishRouting("router message two" + strconv.Itoa(i))
	}
	rabbitMQRoutingOne.Destory()
	rabbitMQRoutingTwo.Destory()
}

func TestRabbitMQRoutingRecieve(t *testing.T) {

	//接收者
	rabbitMQRouting, err := NewRabbitMQRouting("routerEx", "router_one")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	rabbitMQRouting.RecieveRouting()
	rabbitMQRouting.Destory()
}

func TestRabbitMQTopic(t *testing.T) {
	//Topic模式发送者
	rabbitMQTopicOne, err := NewRabbitMQTopic("topicEx", "test.topic.one")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	rabbitMQTopicTwo, err := NewRabbitMQTopic("topicEx", "test.topic.tuo")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}

	for i := 0; i <= 10; i++ {
		rabbitMQTopicOne.PublishTopic("topic one message " + strconv.Itoa(i))
		rabbitMQTopicTwo.PublishTopic("topic two message " + strconv.Itoa(i))
	}
	rabbitMQTopicOne.Destory()
	rabbitMQTopicTwo.Destory()
}

func TestRabbitMQTopicRecieve(t *testing.T) {
	//Topic接收者
	rabbitMQTopic, err := NewRabbitMQTopic("topicEx", "#")
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ:%v", err)
	}
	rabbitMQTopic.RecieveTopic()
	rabbitMQTopic.Destory()
}
