package lib

import (
	"log"
	"math/rand/v2"
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
	NodesMetaInfoMutex *sync.Mutex //TODO: change this to a RWMutex
	otherNodes         []string
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	store := &MessageStore{MessageMap: make(map[int]bool), messages: NewAVLTRee[*MessageItem]()}
	self := &NodeState{msgLock: &sync.RWMutex{}, NodesMetaInfoMutex: &sync.Mutex{}, messageStore: store, node: node}
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
	log.Println("..starting the BackgroundSync")
	for {
		time.Now()
		nodeToSync := getRandomNodes(self.otherNodes)
		time.Sleep(1000 * time.Millisecond)
		self.NodesMetaInfoMutex.Lock()
		nodeMeta, exists := self.OtherNodesMetaInfo[nodeToSync]
		self.NodesMetaInfoMutex.Unlock()
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

func getRandomNodes(otherNodes []string) string {
	n := len(otherNodes)
	return otherNodes[rand.IntN(n)]
}

func (self *NodeState) ReadMessages(callback func(messages *MessageStore)) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messageStore)

}
