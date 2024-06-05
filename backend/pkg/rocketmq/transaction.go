package rocketmq

import (
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type TransactionDemoListener struct {
}

func (dl *TransactionDemoListener) ExecuteLocalTransaction(msg *primitive.Message) primitive.LocalTransactionState {
	fmt.Println("开始执行本地逻辑")
	time.Sleep(time.Second * 3)
	fmt.Println("执行本地逻辑失败")
	//本地执行逻辑无缘无故失败 代码异常 宕机
	return primitive.UnknowState
}

func (dl *TransactionDemoListener) CheckLocalTransaction(msg *primitive.MessageExt) primitive.LocalTransactionState {
	fmt.Println("rocketmq的消息回查提交")
	return primitive.CommitMessageState
	//fmt.Println("rocketmq的消息回查回滚")
	//return primitive.RollbackMessageState
}
