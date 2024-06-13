package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestGroupConsumer(t *testing.T) {
	//newConsumer()
	groupConsumer()
}

func groupConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	brokers := "localhost:9092"
	group := "xuch-group"
	// 创建一个消费者组
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		fmt.Printf("Error creating consumer group client: %v", err)
	}

	consumer := TestConsumer{
		ready: make(chan bool),
	}

	topics := []string{"test_xuch2"}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					fmt.Printf("Error from consumer: %v\n", err)
					return
				}
				fmt.Printf("Error from consumer: %v\n", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	fmt.Println("Sarama consumer up and running!...")

	//time.Sleep(time.Second * 30)

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	consumptionIsPaused := false
	for keepRunning {
		select {
		case <-ctx.Done():
			fmt.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			fmt.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}

	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		fmt.Printf("Error closing client: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type TestConsumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *TestConsumer) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *TestConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *TestConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {

		select {
		case message, ok := <-claim.Messages():
			if !ok {
				fmt.Printf("message channel was closed")
				return nil
			}
			fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			session.Commit()
		case <-session.Context().Done():
			return nil
		}
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		fmt.Printf("Resuming consumption")
	} else {
		client.PauseAll()
		fmt.Printf("Pausing consumption")
	}

	*isPaused = !*isPaused
}

func newConsumer() {
	customer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		fmt.Println("failed init customer,err:", err)
		return
	}

	partitionlist, err := customer.Partitions("test_xuch2") //获取topic的所有分区
	if err != nil {
		fmt.Println("failed get partition list,err:", err)
		return
	}

	fmt.Println("partitions:", partitionlist)

	for partition := range partitionlist { // 遍历所有分区
		//根据消费者对象创建一个分区对象
		pc, err := customer.ConsumePartition("test_xuch2", int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Println("failed get partition consumer,err:", err)
			return
		}
		defer pc.Close() // 移动到这里

		go func(consumer sarama.PartitionConsumer) {
			defer pc.AsyncClose() // 移除这行，因为已经在循环结束时关闭了
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v", msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(pc)
		time.Sleep(time.Second * 10)
	}
}
