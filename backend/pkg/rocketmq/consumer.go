package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type rocketConsumer struct {
	addrs    []string
	group    string
	consumer rocketmq.PushConsumer
}

func NewRocketConsumer(addrs []string, group string, opts ...consumer.Option) (*rocketConsumer, error) {
	var c rocketmq.PushConsumer
	var err error
	if len(opts) == 0 {
		c, err = rocketmq.NewPushConsumer(
			consumer.WithGroupName(group),
			consumer.WithNsResolver(primitive.NewPassthroughResolver(addrs)),
		)
	} else {
		opts = append(opts, consumer.WithGroupName(group), consumer.WithNsResolver(primitive.NewPassthroughResolver(addrs)))
		c, err = rocketmq.NewPushConsumer(opts...)
	}
	if err != nil {
		fmt.Printf("create consumer error: %s", err)
		return nil, err
	}
	return &rocketConsumer{
		addrs:    addrs,
		group:    group,
		consumer: c,
	}, nil
}

func (c *rocketConsumer) Close() {
	consumer := c.consumer
	if consumer != nil {
		err := consumer.Shutdown()
		if err != nil {
			fmt.Printf("consumer shutdown err:%v\n", err)
		}
	}
}

func (c *rocketConsumer) SubscribeCust(topic string, selector consumer.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error {
	rConsumer := c.consumer
	err := rConsumer.Subscribe(topic, selector, f)
	if err != nil {
		fmt.Printf("consumer subscribe err:%v\n", err)
		return err
	}
	err = rConsumer.Start()
	if err != nil {
		fmt.Printf("consumer start err:%v\n", err)
		return err
	}
	return nil
}

func (c *rocketConsumer) Subscribe(topic string) error {
	rConsumer := c.consumer
	err := rConsumer.Subscribe(topic,
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		fmt.Printf("consumer subscribe err:%v\n", err)
		return err
	}
	err = rConsumer.Start()
	if err != nil {
		fmt.Printf("consumer start err:%v\n", err)
		return err
	}
	return nil
}
