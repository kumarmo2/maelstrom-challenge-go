package lib

import (
	"log"
	"strconv"
	"strings"
	"sync"
)

const AppendLogEvent string = "append-log"

type LogEvent struct {
	key  string
	item *LogItem
}

type KafkaLog struct {
	key                 string
	storage             *AVLTree[*LogItem]
	lock                *sync.RWMutex
	incrOffsetBy        int
	offset              int
	committedOffset     int
	committedOffsetLock *sync.RWMutex // TODO: in multi node kafka workload, this lock will be replaced by a linKV
	ns                  *NodeState
}

func NewLog(key string, ns *NodeState) *KafkaLog {
	totalNodes := len(ns.node.NodeIDs())
	currNode := ns.node.ID()
	numStr := strings.TrimPrefix(currNode, "n")
	i, err := strconv.Atoi(numStr)
	if err != nil {
		log.Fatalf("error while converting string to int, currNode: %v, err: %v, ", currNode, err)
	}

	// TODO: for multi node, we will need to calculate initOffset and incrOffsetBy
	return &KafkaLog{key: key, storage: NewAVLTRee[*LogItem](), lock: &sync.RWMutex{},
		offset: i, incrOffsetBy: totalNodes, ns: ns, committedOffsetLock: &sync.RWMutex{}}
}

func (self *KafkaLog) CommitOffset(offset int) {
	self.committedOffsetLock.Lock()
	defer self.committedOffsetLock.Unlock()

	self.committedOffset = offset
}

func (self *KafkaLog) GetCommitOffset() int {
	self.committedOffsetLock.RLock()
	defer self.committedOffsetLock.RUnlock()
	return self.committedOffset

}

func (self *KafkaLog) GetAllFrom(offset int) [][]any {
	self.lock.RLock()
	defer self.lock.RUnlock()
	items := self.storage.GetItemsGreaterThanAndIncludingInOrder(offset)
	result := make([][]any, len(items))

	for i, v := range items {
		slice := make([]any, 2)
		slice[0] = v.offset
		slice[1] = v.msg
		result[i] = slice
	}

	return result
}

func (log *KafkaLog) Append(msg any) int {
	log.lock.Lock()
	defer log.lock.Unlock()

	log.offset += log.incrOffsetBy

	item := &LogItem{msg: msg, offset: log.offset}
	log.storage.InsertItem(item)

	return log.offset
}

type LogItem struct {
	offset int
	msg    any
}

func (self *LogItem) Key() int {
	return self.offset
}
