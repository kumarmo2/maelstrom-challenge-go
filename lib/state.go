package lib

import (
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type GossipSendData struct {
	Type     string         `json:"type"`
	Messages []*MessageItem `json:"msgs"`
}

type GossipSendDataAck struct {
	Type     string    `json:"type"`
	LastSync time.Time `json:"lastSync"`
}

type MessageItem struct {
	Num  int       `json:"num"`
	Time time.Time `json:"time"`
}

type NodeMetaInfo struct {
	name     string
	LastSync time.Time
}

func newMessageItem(num int) *MessageItem {
	return &MessageItem{Num: num, Time: time.Now()}
}

func (message *MessageItem) Key() int {
	return int(message.Time.UnixMilli())
}

type MessageStore struct {
	MessageMap map[int]bool
	messages   *AVLTree[*MessageItem]
}

func (self *MessageStore) InsertItem(message int) {
	if _, exists := self.MessageMap[message]; exists {
		return
	}
	msg := newMessageItem(message)
	self.MessageMap[message] = true
	self.messages.InsertItem(msg)
}

func (self *MessageStore) insertMessageItem(message *MessageItem) {
	if _, exists := self.MessageMap[message.Num]; exists {
		return
	}
	self.MessageMap[message.Num] = true
	self.messages.InsertItem(message)
}

func (self *MessageStore) ContainsKey(message int) bool {
	_, exists := self.MessageMap[message]
	return exists
}
func (store *MessageStore) GetItemsGreaterThan(key int) []*MessageItem {
	messages := store.messages.GetItemsGreaterThanInOrder(key)
	return messages
}

type NodeState struct {
	msgLock            *sync.RWMutex
	messageStore       *MessageStore
	nodeId             string
	node               *maelstrom.Node
	OtherNodesMetaInfo map[string]*NodeMetaInfo
	// NodesMetaInfoMutex *sync.Mutex //TODO: change this to a RWMutex
	NodesMetaInfoLock *sync.RWMutex //TODO: change this to a RWMutex
	otherNodes        []string
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	store := &MessageStore{MessageMap: make(map[int]bool), messages: NewAVLTRee[*MessageItem]()}
	self := &NodeState{msgLock: &sync.RWMutex{}, NodesMetaInfoLock: &sync.RWMutex{}, messageStore: store, node: node}
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
func (self *NodeState) InsertMessageItem(message *MessageItem) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	self.messageStore.insertMessageItem(message)
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	// msg := newMessageItem(message)
	self.messageStore.InsertItem(message)
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
			msgs := self.messageStore.GetItemsGreaterThan(lastSync)
			if len(msgs) < 1 {
				continue
			}
			body := &GossipSendData{Type: "gossip-send-data", Messages: msgs}
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

func (self *NodeState) ReadMessages(callback func(messages *MessageStore)) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messageStore)

}
