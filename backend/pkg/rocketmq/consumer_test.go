package rocket

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"testing"
)

func TestComsumerNormalMsg(t *testing.T) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group-normal-xuch"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		fmt.Printf("create consumer error: %s", err)
	}
	if err := c.Subscribe("test-xuch",
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
	}
	err = c.Start()
	defer c.Shutdown()
	if err != nil {
		fmt.Printf("start consumer error: %s", err)
	}
	var end chan struct{}
	<-end
}

func TestComsumerAsyncMsg(t *testing.T) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group-async-xuch"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		fmt.Printf("create consumer error: %s", err)
	}
	if err := c.Subscribe("test-async-xuch",
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
	}
	err = c.Start()
	defer c.Shutdown()
	if err != nil {
		fmt.Printf("start consumer error: %s", err)
	}
	var end chan struct{}
	<-end
}

func TestComsumerDelayMsg(t *testing.T) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group-delay-xuch"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		panic(err)
	}
	topic := "test-delay-xuch"
	if err := c.Subscribe(topic,
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
	}
	err = c.Start()
	defer c.Shutdown()
	if err != nil {
		fmt.Printf("start consumer error: %s", err)
	}
	var end chan struct{}
	<-end
}

func TestComsumerTransactionMsg(t *testing.T) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group-transaction-xuch"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		panic(err)
	}
	topic := "test-transaction-xuch"
	if err := c.Subscribe(topic,
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
	}
	err = c.Start()
	defer c.Shutdown()
	if err != nil {
		fmt.Printf("start consumer error: %s", err)
	}
	var end chan struct{}
	<-end
}

func TestComsumerFifoMsg(t *testing.T) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName("group-fifo-xuch"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		panic(err)
	}
	topic := "test-fifo-xuch"
	if err := c.Subscribe(topic,
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
	}
	err = c.Start()
	defer c.Shutdown()
	if err != nil {
		fmt.Printf("start consumer error: %s", err)
	}
	var end chan struct{}
	<-end
}
