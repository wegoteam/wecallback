package active

import (
	"fmt"
	"github.com/go-stomp/stomp"
	"testing"
	"time"
)

func TestSendDemo(t *testing.T) {
	conn, err := stomp.Dial("tcp", "localhost:61613", stomp.ConnOpt.Login("admin", "admin"), stomp.ConnOpt.HeartBeat(0, 0))
	if err != nil {
		fmt.Printf("connect error %v\n", err)
	}

	err = conn.Send("/queue/test", "text/plain", []byte("test send activemq"))
	if err != nil {
		fmt.Printf("send error %v\n", err)
	}
	time.Sleep(time.Second * 3)
	err = conn.Disconnect()
	if err != nil {
		fmt.Printf("disconnect error %v\n", err)
	}
}

func TestSendQueue(t *testing.T) {
	activeMQ := NewActiveMQ("localhost:61613")
	err := activeMQ.Send("/queue/test", "active msg")
	if err != nil {
		fmt.Printf("send error %v\n", err)
	}
}

func TestReceiveQueueDemo(t *testing.T) {
	conn, err := stomp.Dial("tcp", "localhost:61613")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Disconnect()

	sub, err := conn.Subscribe("/queue/test", stomp.AckAuto)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer sub.Unsubscribe()

	for {
		msg := <-sub.C
		fmt.Printf("Received message: %s\n", string(msg.Body))
	}
}

func TestReceiveQueue(t *testing.T) {
	activeMQ := NewActiveMQ("localhost:61613")
	err := activeMQ.Subscribe("/queue/test", stomp.AckClient, func(msg *stomp.Message, con *stomp.Conn, err error) {
		if err != nil {
			fmt.Printf("Received error %v\n", err)
			return
		}
		fmt.Printf("Received message: %s\n", string(msg.Body))
		err = con.Ack(msg)
		if err != nil {
			fmt.Printf("ack error %v\n", err)
		}
	})
	if err != nil {
		fmt.Printf("subscribe error) %v\n", err)
	}
}
