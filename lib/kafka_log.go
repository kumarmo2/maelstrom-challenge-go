package lib

import (
	"log"
	"strconv"
	"strings"
	"sync"
)

const ()

type LogEvent struct {
	eventType int
	item      *logItem
}

type KafkaLog[T any] struct {
	key                 string
	storage             *AVLTree[*logItem]
	lock                *sync.RWMutex
	incrOffsetBy        int
	offset              int
	committedOffset     int
	committedOffsetLock *sync.RWMutex // TODO: in multi node kafka workload, this lock will be replaced by a linKV
	ns                  *NodeState[T]
}

func NewLog[T any](key string, ns *NodeState[T]) *KafkaLog[T] {
	totalNodes := len(ns.node.NodeIDs())
	currNode := ns.node.ID()
	numStr := strings.TrimPrefix(currNode, "n")
	i, err := strconv.Atoi(numStr)
	if err != nil {
		log.Fatalf("error while converting string to int, currNode: %v, err: %v, ", currNode, err)
	}

	// TODO: for multi node, we will need to calculate initOffset and incrOffsetBy
	return &KafkaLog[T]{key: key, storage: NewAVLTRee[*logItem](), lock: &sync.RWMutex{},
		offset: i, incrOffsetBy: totalNodes, ns: ns, committedOffsetLock: &sync.RWMutex{}}
}

func (self *KafkaLog[T]) CommitOffset(offset int) {
	self.committedOffsetLock.Lock()
	defer self.committedOffsetLock.Unlock()

	self.committedOffset = offset
}

func (self *KafkaLog[T]) GetCommitOffset() int {
	self.committedOffsetLock.RLock()
	defer self.committedOffsetLock.RUnlock()
	return self.committedOffset

}

func (self *KafkaLog[T]) GetAllFrom(offset int) [][]any {
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

func (log *KafkaLog[T]) Append(msg any) int {
	log.lock.Lock()
	defer log.lock.Unlock()

	log.offset += log.incrOffsetBy

	item := &logItem{msg: msg, offset: log.offset}
	log.storage.InsertItem(item)

	return log.offset
}

type logItem struct {
	offset int
	msg    any
}

func (self *logItem) Key() int {
	return self.offset
}
