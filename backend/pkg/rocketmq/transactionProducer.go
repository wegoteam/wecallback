package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"time"
)

type rocketTransactionProducer struct {
	addrs               []string
	opts                []producer.Option
	transactionProducer rocketmq.TransactionProducer
}

func NewRocketTransactionProducer(addrs []string, listener primitive.TransactionListener, opts []producer.Option) (*rocketTransactionProducer, error) {
	var p rocketmq.TransactionProducer
	var err error
	if len(opts) == 0 {
		p, err = rocketmq.NewTransactionProducer(
			listener,
			producer.WithNsResolver(primitive.NewPassthroughResolver(addrs)),
			producer.WithRetry(3),
			producer.WithSendMsgTimeout(30*time.Second),
		)
	} else {
		opts = append(opts, producer.WithNsResolver(primitive.NewPassthroughResolver(addrs)))
		p, err = rocketmq.NewTransactionProducer(listener, opts...)
	}
	if err != nil {
		fmt.Printf("new producer err:%v\n", err)
		return nil, err
	}
	if err = p.Start(); err != nil {
		fmt.Printf("start producer err:%v\n", err)
		return nil, err
	}
	return &rocketTransactionProducer{
		addrs:               addrs,
		opts:                opts,
		transactionProducer: p,
	}, nil
}

func (p *rocketTransactionProducer) Close() {
	transactionProducer := p.transactionProducer
	if transactionProducer != nil {
		err := transactionProducer.Shutdown()
		if err != nil {
			fmt.Printf("producer shutdown err:%v\n", err)
		}
	}
}

func (tp *rocketTransactionProducer) SendTransactionMessage(topic, data string) {
	transactionProducer := tp.transactionProducer
	// 构建一个消息
	message := primitive.NewMessage(topic, []byte(data))
	res, err := transactionProducer.SendMessageInTransaction(context.Background(), message)
	if err != nil {
		fmt.Printf("send message error: %s\n", err)
	} else {
		fmt.Printf("send message success: result=%s\n", res.String())
	}
}
