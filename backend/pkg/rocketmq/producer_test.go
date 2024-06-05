package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"testing"
	"time"
)

func TestNormalProducer(t *testing.T) {
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2), //指定重试次数
	)
	//p, err := rocketmq.NewProducer(producer.WithNameServer([]string{"127.0.0.1:9876"}))
	if err != nil {
		panic(err)
	}
	if err = p.Start(); err != nil {
		panic("启动producer失败")
	}
	topic := "test-xuch"
	// 构建一个消息
	message := primitive.NewMessage(topic, []byte("hello rocketmq"))
	res, err := p.SendSync(context.Background(), message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
	if err = p.Shutdown(); err != nil {
		fmt.Printf("shutdown producer error: %s\n", err)
	}
}

func TestAsyncProducer(t *testing.T) {
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2), //指定重试次数
	)
	if err != nil {
		fmt.Printf("create producer error: %s\n", err)
	}
	if err = p.Start(); err != nil {
		fmt.Printf("start producer error: %s\n", err)
	}
	topic := "test-async-xuch"
	// 构建一个消息
	message := primitive.NewMessage(topic, []byte("hello rocketmq async msg"))

	err = p.SendAsync(context.Background(), func(ctx context.Context, result *primitive.SendResult, err error) {
		if err != nil {
			fmt.Printf("send message error: %s\n", err)
		} else {
			fmt.Printf("send message success: result=%s\n", result.String())
		}
	}, message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	}

	time.Sleep(time.Second * 10)
	if err = p.Shutdown(); err != nil {
		fmt.Printf("shutdown producer error: %s\n", err)
	}
}

func TestDelayProducer(t *testing.T) {
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2), //指定重试次数
	)
	if err != nil {
		panic(err)
	}
	if err = p.Start(); err != nil {
		panic("启动producer失败")
	}
	topic := "test-delay-xuch"
	// 构建一个消息
	message := primitive.NewMessage(topic, []byte("this is a delay message 2"))
	// 给message设置延迟级别
	message.WithDelayTimeLevel(3)
	res, err := p.SendSync(context.Background(), message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
	if err = p.Shutdown(); err != nil {
		fmt.Printf("shutdown producer error: %s\n", err)
	}
}

func TestTransactionProducer(t *testing.T) {
	p, _ := rocketmq.NewTransactionProducer(
		&TransactionDemoListener{},
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(1),
	)
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s\n", err.Error())
	}
	topic := "test-transaction-xuch"
	res, err := p.SendMessageInTransaction(context.Background(),
		primitive.NewMessage(topic, []byte("Hello RocketMQ again 2222")))
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}

	time.Sleep(5 * time.Minute)
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
	}
}

func TestFifoProducer(t *testing.T) {
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2), //指定重试次数
	)
	if err != nil {
		panic(err)
	}
	if err = p.Start(); err != nil {
		panic("启动producer失败")
	}
	topic := "test-fifo-xuch"
	//发送消息
	for i := 0; i < 10; i++ {
		msg := &primitive.Message{
			Topic: topic,
			Body:  []byte(fmt.Sprintf("Message %d", i)),
		}
		res, err := p.SendSync(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("Send message success, Message ID: %s\n", res.MsgID)
	}
	if err = p.Shutdown(); err != nil {
		fmt.Printf("shutdown producer error: %s\n", err)
	}
}
