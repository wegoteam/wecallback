package kafka

import (
	"github.com/pkg/errors"
	"slices"
	"sync"
	"time"
)

// 节点、主题、消费组、分区、副本
var (
	brokerMap   = make(map[string][]KafkaBroker)
	topicMap    = make(map[string][]KafkaTopic)
	comsumerMap = make(map[string][]KafkaComsumer)
)
var (
	brokerRW   sync.RWMutex
	topicRW    sync.RWMutex
	comsumerRW sync.RWMutex
)

// KafkaBroker
// @Description: kafka 节点
type KafkaBroker struct {
	brokerid string // 节点id
	id       string // kafka节点ID
	name     string // kafka节点名称
	address  string // kafka节点地址
}

// KafkaTopic
// @Description: kafka 主题
type KafkaTopic struct {
	name       string            // 主题名称
	msgTotal   uint64            // 消息总数
	partitions []KafkaPartition  // 分区
	configs    map[string]string // 主题配置
}

// KafkaPartition
// @Description: kafka 分区
type KafkaPartition struct {
	id     int32  // 分区id
	leader string //分区leader
}

// KafkaComsumer
// @Description: kafka 消费者
type KafkaComsumer struct {
	name    string      // 消费者名称
	active  bool        // 消费者是否在线
	offsets KafkaOffset //偏移量
}

// KafkaOffset
// @Description: kafka 偏移量
type KafkaOffset struct {
	topic      string    //主题名称
	partition  uint32    //分区
	offset     uint32    //偏移量
	start      uint32    //开始偏移量
	end        uint32    //结束偏移量
	lag        uint32    //延迟
	lastcommit time.Time //最后提交时间
}

// GetBrokers
// @Description: 获取kafka节点信息
// @return map[string][]KafkaBroker
func GetBrokers() map[string][]KafkaBroker {
	brokerRW.RLock()
	defer brokerRW.RUnlock()
	return brokerMap
}

// AddBroker
// @Description: 添加kafka节点信息
// @param: brokerid 节点id
// @param: id kafka节点ID
// @param: name kafka节点名称
// @param: address kafka节点地址
// @return error
func AddBroker(brokerid, id, name, address string) error {
	brokerRW.Lock()
	defer brokerRW.Unlock()
	brokers, ok := brokerMap[brokerid]
	if ok {
		slices.DeleteFunc(brokers, func(b KafkaBroker) bool {
			return b.address == address
		})
	}
	b := KafkaBroker{
		brokerid: brokerid,
		id:       id,
		name:     name,
		address:  address,
	}
	brokers = append(brokers, b)
	brokerMap[brokerid] = brokers
	return nil
}

// DelBroker
// @Description: 删除kafka节点信息
// @param: brokerid 节点id
// @param: id kafka节点ID
// @return error
func DelBroker(brokerid, id string) error {
	brokerRW.Lock()
	defer brokerRW.Unlock()
	if brokerid == "" {
		return errors.New("brokerid is empty")
	}
	if id == "" {
		delete(brokerMap, brokerid)
		return nil
	}
	brokers, ok := brokerMap[brokerid]
	if ok {
		slices.DeleteFunc(brokers, func(b KafkaBroker) bool {
			return b.brokerid == brokerid && b.id == id
		})
	}
	brokerMap[brokerid] = brokers
	return nil
}

// GetTopics
// @Description: 获取topic信息
// @return map[string][]KafkaTopic
func GetTopics() map[string][]KafkaTopic {
	topicRW.RLock()
	defer topicRW.RUnlock()
	return topicMap
}

// GetComsumers
// @Description: 获取消费者信息
// @return map[string][]KafkaComsumer
func GetComsumers() map[string][]KafkaComsumer {
	comsumerRW.RLock()
	defer comsumerRW.RUnlock()
	return comsumerMap
}
