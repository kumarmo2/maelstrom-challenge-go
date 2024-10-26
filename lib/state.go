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
	// TODO: see if we can replace uuid with something that takes less bytes on wire.
	return &MessageItem[T]{Message: msg, MessageId: uuid.NewString(), Time: time.Now()}
}

func (message *MessageItem[T]) Key() int {
	return int(message.Time.UnixMilli())
}

type MessageStoreV2[T any] struct {
	MessageMap         map[string]*MessageItem[T] // messageId to
	messages           *AVLTree[*MessageItem[T]]
	OtherNodesMetaInfo map[string]*NodeMetaInfo
	NodesMetaInfoLock  *sync.RWMutex
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
	nodeId     string
	node       *maelstrom.Node
	otherNodes []string
	linKV      *maelstrom.KV
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	self := &NodeState{node: node, linKV: maelstrom.NewLinKV(node)}
	self.nodeId = node.ID()
	allNodes := node.NodeIDs()

	// self.OtherNodesMetaInfo = make(map[string]*NodeMetaInfo)
	self.otherNodes = make([]string, 0)

	for _, n := range allNodes {
		if n != self.nodeId {
			// self.MessageStoreV2.OtherNodesMetaInfo[n] = &NodeMetaInfo{name: n, LastSync: time.Now().AddDate(0, 0, -1)}
			self.otherNodes = append(self.otherNodes, n)
		}
	}
	// go self.BackgroundSync() //TODO: for multi-node kafka workload, we will start the BackgroundSync
	return self
}

func getRandomNodes(otherNodes []string) []string {
	randNodes := make([]string, 0)
	n := len(otherNodes)
	for {
		len := len(randNodes)
		// TODO: we should be dynamically calculating how many nodes we want to sync to in background
		if len == 1 {
			return randNodes
		}
		node := otherNodes[rand.IntN(n)]
		if slices.Contains(randNodes, node) {
			continue
		}
		randNodes = append(randNodes, node)
	}

}
