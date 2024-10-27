package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const AppendLogEvent string = "append-log-event"

type LogEvent struct {
	Key       string          `json:"key"`
	EventType string          `json:"event_type"`
	Msg       json.RawMessage `json:"msg"`
}

type LogStorage struct {
	MessageMap map[int]*LogItem
	messages   *AVLTree2[int, *LogItem]
}
type logSyncInfo struct {
	node           string
	lock           *sync.RWMutex
	lastSyncOffset int
}

type KafkaLog struct {
	key                 string
	ownerNodeId         string
	storage             *LogStorage
	lock                *sync.RWMutex
	incrOffsetBy        int
	offset              int
	committedOffset     int
	committedOffsetLock *sync.RWMutex // TODO: in multi node kafka workload, this lock will be replaced by a linKV
	ns                  *NodeState
	nodeSyncInfo        *NicheThreadSafeMap[string, *logSyncInfo]
}

func NewLog(key string, ns *NodeState, ownerNodeId string) *KafkaLog {
	totalNodes := len(ns.node.NodeIDs())
	currNode := ns.node.ID()
	numStr := strings.TrimPrefix(currNode, "n")
	i, err := strconv.Atoi(numStr)
	if err != nil {
		log.Fatalf("error while converting string to int, currNode: %v, err: %v, ", currNode, err)
	}

	storage := &LogStorage{MessageMap: make(map[int]*LogItem), messages: NewAVL2Tree[int, *LogItem]()}

	// TODO: for multi node, we will need to calculate initOffset and incrOffsetBy
	log := &KafkaLog{key: key, storage: storage, lock: &sync.RWMutex{},
		offset: i, incrOffsetBy: totalNodes, ns: ns, committedOffsetLock: &sync.RWMutex{},
		nodeSyncInfo: NewNicheThreadSafeMap[string, *logSyncInfo](), ownerNodeId: ownerNodeId}
	if log.IsLeader() {
		go log.BackgroundSync()
	}
	return log
}

func (self *KafkaLog) CommitOffset(offset int) {
	if !self.IsLeader() {
		log.Fatalf("KafkaLog.CommitOffset: node %v, is not the leader of the log: %v", self.ns.node.ID(), self.key)
	}
	self.committedOffsetLock.Lock()
	defer self.committedOffsetLock.Unlock()

	self.committedOffset = offset

	// log.Printf("KafkaLog.CommitOffset: key: %v, committing offset: %v", self.key, self.committedOffset)
}

func (self *KafkaLog) GetCommitOffset() int {
	if !self.IsLeader() {
		log.Fatalf("KafkaLog.GetCommitOffset: node %v, is not the leader of the log: %v", self.ns.node.ID(), self.key)
	}
	self.committedOffsetLock.RLock()
	defer self.committedOffsetLock.RUnlock()
	res := self.committedOffset
	// log.Printf("KafkaLog.GetCommitOffset: key: %v, returning offsetf: %v ", self.key, res)
	return res

}

func (self *KafkaLog) GetAllFrom(offset int) [][]any {
	self.lock.RLock()
	defer self.lock.RUnlock()
	items := self.storage.messages.GetItemsGreaterThanAndIncludingInOrder(offset)
	result := make([][]any, len(items))

	for i, v := range items {
		slice := make([]any, 2)
		slice[0] = v.Offset
		slice[1] = v.Msg
		result[i] = slice
	}

	return result
}

func generateFuncForLogSyncInfo(node string) func() *logSyncInfo {
	return func() *logSyncInfo {
		return &logSyncInfo{node: node, lastSyncOffset: -1, lock: &sync.RWMutex{}}
	}

}

func (self *KafkaLog) IsLeader() bool {
	return self.ownerNodeId == self.ns.node.ID()
}

func (self *KafkaLog) BackgroundSync() {
	if !self.IsLeader() {
		e := fmt.Sprintf("node: %v is not the leader of the log: %v", self.ns.node.ID(), self.key)
		log.Fatalf(e)
	}
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			for _, node := range self.ns.otherNodes {
				syncInfo := self.nodeSyncInfo.GetOrCreateAndThenGet(node, generateFuncForLogSyncInfo(node))
				self.lock.RLock()
				syncInfo.lock.RLock()
				unSyncedMesages := self.storage.messages.GetItemsGreaterThanInOrder(syncInfo.lastSyncOffset)
				syncInfo.lock.RUnlock()
				self.lock.RUnlock()
				if len(unSyncedMesages) < 1 {
					continue
				}
				msgs, err := json.Marshal(unSyncedMesages)
				if err != nil {
					panic(err)
				}
				body := LogEvent{
					Key:       self.key,
					EventType: AppendLogEvent,
					Msg:       msgs,
				}
				var req *CustomMessage[*LogEvent] = &CustomMessage[*LogEvent]{
					Type: "log-event",
					Body: &body,
				}
				self.ns.node.Send(node, req)
			}
		}
	}()
}

func (self *KafkaLog) HandleLogEvent(event *LogEvent) error {
	if event == nil {
		return errors.New("event cannot be nil")
	}
	if event.Key != self.key {
		return errors.New(fmt.Sprint("key doesn't match for the log event "))
	}
	if event.EventType == AppendLogEvent {
		// log.Printf("!!! got AppendLogEvent for key: %v", self.key)
		var reqBody []*LogItem
		err := json.Unmarshal(event.Msg, &reqBody)
		if err != nil {
			return err
		}
		return self.SyncLogItems(reqBody)

	} else {
		// TODO: change this panic to returning error
		log.Fatalf("event type: %v, is not handled.", event.EventType)
	}
	panic("unreachable")
}

func (self *KafkaLog) HandleLogSyncAck(req *LogSyncAck) error {
	// log.Printf(">>> KafkaLog.HandleLogSyncAck: for logf: %v, got the log sync event, req.offsetf; %v", self.key, req.Offset)
	// TODO: add validations
	nodeSyncInfo := self.nodeSyncInfo.GetOrCreateAndThenGet(req.Follower, generateFuncForLogSyncInfo(req.Follower))
	nodeSyncInfo.lock.Lock()
	defer nodeSyncInfo.lock.Unlock()
	if req.Offset > nodeSyncInfo.lastSyncOffset {
		// log.Printf(">>> KafkaLog.HandleLogSyncAck: for logf: %v, updating the offset, req.offsetf; %v", self.key, req.Offset)
		nodeSyncInfo.lastSyncOffset = req.Offset
	}
	return nil

}

func (self *KafkaLog) SyncLogItems(msgs []*LogItem) error {
	if msgs == nil {
		return errors.New("msgs cannot be nil")
	}

	self.lock.Lock()
	for _, msg := range msgs {
		self.storage.messages.InsertItem(msg)
		if self.offset < msg.Offset {
			self.offset = msg.Offset
			// log.Printf("!!!, SyncLogItems, for key: %v, syncing the offset to %v ", self.key, self.offset)
		}
	}
	req := &CustomMessage[*LogSyncAck]{
		Type: "log-sync-ack",
		Body: &LogSyncAck{
			Key:      self.key,
			Offset:   self.offset,
			Follower: self.ns.node.ID(),
		},
	}
	self.lock.Unlock()
	return self.ns.node.RPC(self.ownerNodeId, req, func(msg maelstrom.Message) error { return nil })
}

func (self *KafkaLog) appendLogItem(msg any) *LogItem {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.offset += self.incrOffsetBy

	item := &LogItem{Msg: msg, Offset: self.offset}
	self.storage.messages.InsertItem(item)
	return item
}

func (self *KafkaLog) Append(msg any) *LogItem {
	if !self.IsLeader() {
		log.Fatalf("KafkaLog.Append, node: %v, is not the leader of the log: %v", self.ns.node.ID(), self.key)
	}
	return self.appendLogItem(msg)
}

type LogItem struct {
	Offset int `json:"offset"`
	Msg    any `json:"msg"`
}

func (self *LogItem) Key() int {
	return self.Offset
}
