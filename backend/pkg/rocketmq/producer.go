package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"time"
)

type rocketProducer struct {
	addrs    []string
	opts     []producer.Option
	producer rocketmq.Producer
}

func NewRocketProducer(addrs []string, opts []producer.Option) (*rocketProducer, error) {
	var p rocketmq.Producer
	var err error
	if len(opts) == 0 {
		p, err = rocketmq.NewProducer(
			producer.WithNsResolver(primitive.NewPassthroughResolver(addrs)),
			producer.WithRetry(3),
			producer.WithSendMsgTimeout(30*time.Second),
		)
	} else {
		opts = append(opts, producer.WithNsResolver(primitive.NewPassthroughResolver(addrs)))
		p, err = rocketmq.NewProducer(opts...)
	}
	if err != nil {
		fmt.Printf("new producer err:%v\n", err)
		return nil, err
	}
	if err = p.Start(); err != nil {
		fmt.Printf("start producer err:%v\n", err)
		return nil, err
	}
	return &rocketProducer{
		addrs:    addrs,
		opts:     opts,
		producer: p,
	}, nil
}

func (p *rocketProducer) Close() {
	rProducer := p.producer
	if rProducer != nil {
		err := rProducer.Shutdown()
		if err != nil {
			fmt.Printf("producer shutdown err:%v\n", err)
		}
	}
}

func (p *rocketProducer) SendMessage(topic, data string) {
	rProducer := p.producer
	message := primitive.NewMessage(topic, []byte(data))
	res, err := rProducer.SendSync(context.Background(), message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
}

func (p *rocketProducer) SendAsyncMessage(topic, data string) {
	rProducer := p.producer
	message := primitive.NewMessage(topic, []byte(data))
	err := rProducer.SendAsync(context.Background(), func(ctx context.Context, result *primitive.SendResult, err error) {
		if err != nil {
			fmt.Printf("send message error: %s\n", err)
		} else {
			fmt.Printf("send message success: result=%s\n", result.String())
		}
	}, message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	}
}

func (p *rocketProducer) SendDelayMessage(topic, data string, level int) {
	rProducer := p.producer
	message := primitive.NewMessage(topic, []byte(data))
	message.WithDelayTimeLevel(level)
	res, err := rProducer.SendSync(context.Background(), message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
}
