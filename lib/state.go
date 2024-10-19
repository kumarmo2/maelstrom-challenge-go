package lib

import (
	"errors"
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

func (self *MessageStoreV2[T]) insertMessageItem(message *MessageItem[T]) bool {
	if _, exists := self.MessageMap[message.MessageId]; exists {
		return false
	}
	self.MessageMap[message.MessageId] = message
	self.messages.InsertItem(message)
	return true
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
	NodesMetaInfoLock  *sync.RWMutex
	otherNodes         []string
	notifyWhenNewMsgs  chan int
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	store := &MessageStoreV2[int]{MessageMap: make(map[string]*MessageItem[int]), messages: NewAVLTRee[*MessageItem[int]]()}
	self := &NodeState{msgLock: &sync.RWMutex{}, NodesMetaInfoLock: &sync.RWMutex{}, MessageStoreV2: store, node: node, notifyWhenNewMsgs: make(chan int, 200)}
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
func (self *NodeState) InsertMessageItems(messages []*MessageItem[int]) (time.Time, error) {
	if messages == nil {
		return time.Now(), errors.New("messages found nil")
	}
	if len(messages) == 0 {
		return time.Now(), errors.New("empty messages found")
	}
	self.msgLock.Lock()
	defer self.msgLock.Unlock()

	lastSync := messages[0].Time

	for _, msg := range messages {
		if self.MessageStoreV2.insertMessageItem(msg) {
			self.notifyWhenNewMsgs <- msg.Message
		}
		if msg.Time.UnixMilli() >= lastSync.UnixMilli() {
			lastSync = msg.Time
		}
	}
	return lastSync, nil
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	msg := newMessageItem(message)
	if self.MessageStoreV2.insertMessageItem(msg) {
		self.notifyWhenNewMsgs <- msg.Message
	}
}

func (self *NodeState) GetItemsGreaterThan(lastSync time.Time) []*MessageItem[int] {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()

	ls := int(lastSync.UnixMilli())
	return self.MessageStoreV2.GetItemsGreaterThan(ls)
}

func (self *NodeState) BackgroundSync() {
	for {
		nodesToSync := getRandomNodes(self.otherNodes)
		time.Sleep(100 * time.Millisecond)

		for _, nodeToSync := range nodesToSync {
			self.NodesMetaInfoLock.RLock()
			nodeMeta, exists := self.OtherNodesMetaInfo[nodeToSync]
			self.NodesMetaInfoLock.RUnlock()
			if !exists {
				continue
			}
			msgs := self.GetItemsGreaterThan(nodeMeta.LastSync)
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
		if len == 4 {
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
