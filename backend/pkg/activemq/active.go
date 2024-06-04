package activemq

import (
	"time"

	"github.com/go-stomp/stomp"
)

type ActiveMQ struct {
	Addr string
}

var options = []func(*stomp.Conn) error{
	//设置读写超时，超时时间为1个小时
	stomp.ConnOpt.HeartBeat(7200*time.Second, 7200*time.Second),
	stomp.ConnOpt.HeartBeatError(360 * time.Second),
}

// New activeMQ with addr[eg:localhost:61613] as host address.
func NewActiveMQ(addr string) *ActiveMQ {
	if addr == "" {
		addr = "localhost:61613"
	}
	return &ActiveMQ{addr}
}

// Used for health check
func (this *ActiveMQ) Check() error {
	conn, err := this.Connect()
	if err == nil {
		defer conn.Disconnect()
		return nil
	} else {
		return err
	}
}

// Connect to activeMQ
func (this *ActiveMQ) Connect() (*stomp.Conn, error) {
	return stomp.Dial("tcp", this.Addr, options...)
}

// Send msg to destination
func (this *ActiveMQ) Send(destination string, msg string) error {
	conn, err := this.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()
	return conn.Send(
		destination,  // destination
		"text/plain", // content-type
		[]byte(msg))  // body
}

// Subscribe Message from destination
// func handler handle msg reveived from destination
func (this *ActiveMQ) Subscribe(destination string, ack stomp.AckMode, handler func(msg *stomp.Message, con *stomp.Conn, err error)) error {
	conn, err := this.Connect()
	if err != nil {
		return err
	}
	//sub, err := conn.Subscribe(destination, stomp.AckAuto)
	sub, err := conn.Subscribe(destination, ack)
	if err != nil {
		return err
	}
	defer conn.Disconnect()
	defer sub.Unsubscribe()
	for {
		m := <-sub.C
		handler(m, conn, m.Err)
	}
	return err
}

func (this *ActiveMQ) SubscribeAuto(destination string, handler func(msg string, err error)) error {
	conn, err := this.Connect()
	if err != nil {
		return err
	}
	sub, err := conn.Subscribe(destination, stomp.AckAuto)
	if err != nil {
		return err
	}
	defer conn.Disconnect()
	defer sub.Unsubscribe()
	for {
		m := <-sub.C
		handler(string(m.Body), m.Err)
	}
	return err
}
