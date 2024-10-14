package lib

import (
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type GossipSendData[T any] struct {
	Type     string            `json:"type"`
	Messages []*MessageItem[T] `json:"msgs"`
}

type GossipSendDataAck struct {
	Type     string    `json:"type"`
	LastSync time.Time `json:"lastSync"`
}

type MessageItem[T any] struct {
	Message   T         `json:"msg"`
	MessageId string    `json:"msgid"`
	Time      time.Time `json:"time"`
}

type NodeMetaInfo struct {
	name     string
	LastSync time.Time
}

func newMessageItem[T any](msg T) *MessageItem[T] {
	return &MessageItem[T]{Message: msg, MessageId: uuid.NewString(), Time: time.Now()}
}

func (message *MessageItem[T]) Key() int {
	return int(message.Time.UnixMilli())
}

type MessageStoreV2[T any] struct {
	MessageMap map[string]*MessageItem[T] // messageId to
	messages   *AVLTree[*MessageItem[T]]
}

// type MessageStoreV2 struct {
// 	MessageMap map[int]bool
// 	messages   *AVLTree[*MessageItem]
// }

func (self *MessageStoreV2[T]) InsertItem(message *MessageItem[T]) {
	if _, exists := self.MessageMap[message.MessageId]; exists {
		return
	}
	// msg := newMessageItem(message)
	self.MessageMap[message.MessageId] = message
	self.messages.InsertItem(message)
}

func (self *MessageStoreV2[T]) insertMessageItem(message *MessageItem[T]) {
	if _, exists := self.MessageMap[message.MessageId]; exists {
		return
	}
	self.MessageMap[message.MessageId] = message
	self.messages.InsertItem(message)
}

func (self *MessageStoreV2[T]) ContainsKey(message *MessageItem[T]) bool {
	_, exists := self.MessageMap[message.MessageId]
	return exists
}
func (store *MessageStoreV2[T]) GetItemsGreaterThan(key int) []*MessageItem[T] {
	messages := store.messages.GetItemsGreaterThanInOrder(key)
	return messages
}

type NodeState struct {
	msgLock            *sync.RWMutex
	MessageStoreV2     *MessageStoreV2[int]
	nodeId             string
	node               *maelstrom.Node
	OtherNodesMetaInfo map[string]*NodeMetaInfo
	// NodesMetaInfoMutex *sync.Mutex //TODO: change this to a RWMutex
	NodesMetaInfoLock *sync.RWMutex //TODO: change this to a RWMutex
	otherNodes        []string
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	store := &MessageStoreV2[int]{MessageMap: make(map[string]*MessageItem[int]), messages: NewAVLTRee[*MessageItem[int]]()}
	self := &NodeState{msgLock: &sync.RWMutex{}, NodesMetaInfoLock: &sync.RWMutex{}, MessageStoreV2: store, node: node}
	self.nodeId = node.ID()
	allNodes := node.NodeIDs()

	self.OtherNodesMetaInfo = make(map[string]*NodeMetaInfo)
	self.otherNodes = make([]string, 0)

	for _, n := range allNodes {
		if n != self.nodeId {
			self.OtherNodesMetaInfo[n] = &NodeMetaInfo{name: n, LastSync: time.Now().AddDate(0, 0, -1)}
			self.otherNodes = append(self.otherNodes, n)
		}
	}
	go self.BackgroundSync()
	return self
}
func (self *NodeState) InsertMessageItem(message *MessageItem[int]) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	self.MessageStoreV2.insertMessageItem(message)
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	msg := newMessageItem(message)
	self.MessageStoreV2.InsertItem(msg)
}

func (self *NodeState) BackgroundSync() {
	for {
		nodesToSync := getRandomNodes(self.otherNodes)
		time.Sleep(500 * time.Millisecond)

		for _, nodeToSync := range nodesToSync {
			self.NodesMetaInfoLock.RLock()
			nodeMeta, exists := self.OtherNodesMetaInfo[nodeToSync]
			self.NodesMetaInfoLock.RUnlock()
			if !exists {
				continue
			}
			lastSync := int(nodeMeta.LastSync.UnixMilli())
			msgs := self.MessageStoreV2.GetItemsGreaterThan(lastSync)
			if len(msgs) < 1 {
				continue
			}
			body := &GossipSendData[int]{Type: "gossip-send-data", Messages: msgs}
			self.node.Send(nodeToSync, body)

		}
	}
}

func getRandomNodes(otherNodes []string) []string {
	randNodes := make([]string, 0)
	n := len(otherNodes)
	for {
		len := len(randNodes)
		if len == 8 {
			return randNodes
		}
		node := otherNodes[rand.IntN(n)]
		if slices.Contains(randNodes, node) {
			continue
		}

		randNodes = append(randNodes, node)

	}

}

func (self *NodeState) ReadMessages(callback func(messages *MessageStoreV2[int])) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.MessageStoreV2)

}
