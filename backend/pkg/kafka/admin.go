package kafka

import (
	"github.com/pkg/errors"
	"slices"
	"sync"
	"time"
)

// 节点、主题、消费组、分区、副本
var (
	brokerMap   = make(map[string][]webroker)
	topicMap    = make(map[string][]wetopic)
	comsumerMap = make(map[string][]wecomsumer)
)
var (
	brokerRW   sync.RWMutex
	topicRW    sync.RWMutex
	comsumerRW sync.RWMutex
)

// webroker
// @Description: kafka 节点
type webroker struct {
	brokerid string // 节点id
	id       string // kafka节点ID
	name     string // kafka节点名称
	address  string // kafka节点地址
}

// wetopic
// @Description: kafka 主题
type wetopic struct {
	name       string            // 主题名称
	msgTotal   uint64            // 消息总数
	partitions []wepartition     // 分区
	configs    map[string]string // 主题配置
}

// wepartition
// @Description: kafka 分区
type wepartition struct {
	id     int32  // 分区id
	leader string //分区leader
}

// wecomsumer
// @Description: kafka 消费者
type wecomsumer struct {
	name    string     // 消费者名称
	active  bool       // 消费者是否在线
	offsets []weoffset //偏移量
}

// weoffset
// @Description: kafka 偏移量
type weoffset struct {
	topic      string    //主题名称
	partition  uint32    //分区
	offset     uint32    //偏移量
	start      uint32    //开始偏移量
	end        uint32    //结束偏移量
	lag        uint32    //延迟
	lastcommit time.Time //最后提交时间
}

// getBrokers
// @Description: 获取kafka节点信息
// @return map[string][]webroker
func getBrokers() map[string][]webroker {
	brokerRW.RLock()
	defer brokerRW.RUnlock()
	return brokerMap
}

// addBroker
// @Description: 添加kafka节点信息
// @param: brokerid 节点id
// @param: id kafka节点ID
// @param: name kafka节点名称
// @param: address kafka节点地址
// @return error
func addBroker(brokerid, id, name, address string) error {
	if brokerid == "" {
		return errors.New("broker brokerid is empty")
	}
	if address == "" {
		return errors.New("broker address is empty")
	}
	brokerRW.Lock()
	defer brokerRW.Unlock()
	brokers, ok := brokerMap[brokerid]
	if ok {
		slices.DeleteFunc(brokers, func(b webroker) bool {
			return b.address == address
		})
	}
	b := webroker{
		brokerid: brokerid,
		id:       id,
		name:     name,
		address:  address,
	}
	brokers = append(brokers, b)
	brokerMap[brokerid] = brokers
	return nil
}

// delBroker
// @Description: 删除kafka节点信息
// @param: brokerid 节点id
// @param: id kafka节点ID
// @return error
func delBroker(brokerid, id string) error {
	if brokerid == "" {
		return errors.New("brokerid is empty")
	}
	brokerRW.Lock()
	defer brokerRW.Unlock()
	if id == "" {
		delete(brokerMap, brokerid)
		return nil
	}
	brokers, ok := brokerMap[brokerid]
	if ok {
		slices.DeleteFunc(brokers, func(b webroker) bool {
			return b.brokerid == brokerid && b.id == id
		})
	}
	brokerMap[brokerid] = brokers
	return nil
}

// getTopics
// @Description: 获取topic信息
// @return map[string][]wetopic
func getTopics() map[string][]wetopic {
	topicRW.RLock()
	defer topicRW.RUnlock()
	return topicMap
}

// addTopic
// @Description: 添加topic信息
// @param: name
// @param: msgTotal
// @param: partitions
// @param: configs
// @return error
func addTopic(name string, msgTotal uint64, partitions []wepartition, configs map[string]string) error {
	if name == "" {
		return errors.New("broker topic name is empty")
	}
	topicRW.Lock()
	defer topicRW.Unlock()
	topics, ok := topicMap[name]
	if ok {
		slices.DeleteFunc(topics, func(b wetopic) bool {
			return b.name == name
		})
	}
	topic := wetopic{
		name:       name,
		msgTotal:   msgTotal,
		partitions: partitions,
		configs:    configs,
	}
	topicMap[name] = append(topics, topic)
	return nil
}

// delTopic
// @Description: 删除topic信息
// @param: brokerid
// @param: name
// @return error
func delTopic(brokerid, name string) error {
	if brokerid == "" {
		return errors.New("broker brokerid is empty")
	}
	topicRW.Lock()
	defer topicRW.Unlock()
	if name == "" {
		delete(brokerMap, brokerid)
		return nil
	}
	topics, ok := topicMap[brokerid]
	if ok {
		slices.DeleteFunc(topics, func(b wetopic) bool {
			return b.name == name
		})
	}
	topicMap[brokerid] = topics
	return nil
}

// getComsumers
// @Description: 获取消费者信息
// @return map[string][]wecomsumer
func getComsumers() map[string][]wecomsumer {
	comsumerRW.RLock()
	defer comsumerRW.RUnlock()
	return comsumerMap
}

// addComsumer
// @Description: 添加消费者信息
// @param: brokerid
// @param: name
// @param: active
// @param: offsets
// @return error
func addComsumer(brokerid, name string, active bool, offsets []weoffset) error {
	if brokerid == "" {
		return errors.New("broker comsumer brokerid is empty")
	}
	if name == "" {
		return errors.New("broker comsumer name is empty")
	}
	comsumerRW.Lock()
	defer comsumerRW.Unlock()
	comsumers, ok := comsumerMap[brokerid]
	if ok {
		slices.DeleteFunc(comsumers, func(c wecomsumer) bool {
			return c.name == name
		})
	}
	comsumer := wecomsumer{
		name:    name,
		active:  active,
		offsets: offsets,
	}
	comsumerMap[brokerid] = append(comsumers, comsumer)
	return nil
}

// delComsumer
// @Description: 删除消费者信息
// @param: brokerid
// @param: name
// @return error
func delComsumer(brokerid, name string) error {
	if brokerid == "" {
		return errors.New("broker comsumer brokerid is empty")
	}
	if name == "" {
		delete(comsumerMap, brokerid)
		return nil
	}
	comsumerRW.Lock()
	defer comsumerRW.Unlock()
	comsumers, ok := comsumerMap[brokerid]
	if ok {
		slices.DeleteFunc(comsumers, func(c wecomsumer) bool {
			return c.name == name
		})
	}
	comsumerMap[brokerid] = comsumers
	return nil
}
