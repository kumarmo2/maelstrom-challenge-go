package lib

import (
	"log"
	"strconv"
	"strings"
	"sync"
)

const AppendLogEvent string = "append-log"

type Log struct {
}

type LogEvent struct {
	Key       string   `json:"key"`
	Item      *LogItem `json:"item"`
	EventType string   `json:"event_type"`
}

type LogStorage struct {
	MessageMap map[int]*LogItem
	messages   *AVLTree2[int, *LogItem]
}

type KafkaLog struct {
	key                 string
	storage             *LogStorage
	lock                *sync.RWMutex
	incrOffsetBy        int
	offset              int
	committedOffset     int
	committedOffsetLock *sync.RWMutex // TODO: in multi node kafka workload, this lock will be replaced by a linKV
	ns                  *NodeState
	nodeSyncInfo        *ThreadSafeMap[string, *logSyncInfo]
}

func NewLog(key string, ns *NodeState) *KafkaLog {
	totalNodes := len(ns.node.NodeIDs())
	currNode := ns.node.ID()
	numStr := strings.TrimPrefix(currNode, "n")
	i, err := strconv.Atoi(numStr)
	if err != nil {
		log.Fatalf("error while converting string to int, currNode: %v, err: %v, ", currNode, err)
	}

	storage := &LogStorage{MessageMap: make(map[int]*LogItem), messages: NewAVL2Tree[int, *LogItem]()}

	// TODO: for multi node, we will need to calculate initOffset and incrOffsetBy
	return &KafkaLog{key: key, storage: storage, lock: &sync.RWMutex{},
		offset: i, incrOffsetBy: totalNodes, ns: ns, committedOffsetLock: &sync.RWMutex{},
		nodeSyncInfo: NewThreadSafeMap[string, *logSyncInfo]()}
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
	items := self.storage.messages.GetItemsGreaterThanAndIncludingInOrder(offset)
	result := make([][]any, len(items))

	for i, v := range items {
		slice := make([]any, 2)
		slice[0] = v.offset
		slice[1] = v.msg
		result[i] = slice
	}

	return result
}

func generateFuncForLogSyncInfo(node string) func() *logSyncInfo {
	return func() *logSyncInfo {
		return &logSyncInfo{node: node, lastSyncOffset: -1}

	}

}

// func (self *KafkaLog) BackgroundSync() {
//
// 	go func() {
// 		time.Sleep(time.Millisecond * 50)
// 		for _, node := range self.ns.otherNodes {
// 			syncInfo := self.nodeSyncInfo.GetOrCreateAndThenGet(node, generateFuncForLogSyncInfo(node))
// 			self.lock.RLock()
// 			unSyncedMesages := self.storage.messages.GetItemsGreaterThanInOrder(syncInfo.lastSyncOffset)
// 			self.lock.RUnlock()
// 		}
//
// 	}()
// }

func (log *KafkaLog) SyncLogItems(msgs []*LogItem) {
	if msgs == nil {
		return
	}

	log.lock.Lock()
	defer log.lock.Unlock()

	for _, msg := range msgs {
		log.storage.messages.InsertItem(msg)
	}
}

func (log *KafkaLog) Append(msg any) *LogItem {
	log.lock.Lock()
	defer log.lock.Unlock()

	log.offset += log.incrOffsetBy

	item := &LogItem{msg: msg, offset: log.offset}
	log.storage.messages.InsertItem(item)
	return item
}

type LogItem struct {
	offset int
	msg    any
}

func (self *LogItem) Key() int {
	return self.offset
}
