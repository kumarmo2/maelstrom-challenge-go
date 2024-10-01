package lib

import (
	"math/rand/v2"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageItem struct {
	num  int
	time time.Time
}

type NodeMetaInfo struct {
	name     string
	lastSync time.Time
}

func newMessageItem(num int) *MessageItem {
	return &MessageItem{num: num, time: time.Now()}
}

func (message *MessageItem) Key() int {
	return int(message.time.UnixMilli())
}

type MessageStore struct {
	MessageMap map[int]bool
	messages   *AVLTree[*MessageItem]
}

func (self *MessageStore) InsertItem(message int) {
	msg := newMessageItem(message)
	self.MessageMap[message] = true
	self.messages.InsertItem(msg)
}

func (self *MessageStore) ContainsKey(message int) bool {
	_, exists := self.MessageMap[message]
	return exists
}

type NodeState struct {
	msgLock *sync.RWMutex
	// messages           *AVLTree[*MessageItem]
	messageStore       *MessageStore
	nodeId             string
	otherNodesMetaInfo map[string]*NodeMetaInfo
	otherNodes         []string
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	store := &MessageStore{MessageMap: make(map[int]bool), messages: NewAVLTRee[*MessageItem]()}
	self := &NodeState{msgLock: &sync.RWMutex{}, messageStore: store}
	self.nodeId = node.ID()
	allNodes := node.NodeIDs()

	self.otherNodesMetaInfo = make(map[string]*NodeMetaInfo)
	self.otherNodes = make([]string, 0)

	for _, n := range allNodes {
		if n != self.nodeId {
			self.otherNodesMetaInfo[n] = &NodeMetaInfo{name: n, lastSync: time.Now().Add(time.Minute)}
			self.otherNodes = append(self.otherNodes, n)
		}
	}
	// go self.BackgroundSync()
	return self
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	// msg := newMessageItem(message)
	self.messageStore.InsertItem(message)
}

func (self *NodeState) SaveBroadcastMessageIfNew(message int, node *maelstrom.Node) error {
	self.msgLock.RLock()
	exists := self.messageStore.ContainsKey(message)
	if exists {
		// No need to broadcast further.
		self.msgLock.RUnlock()
		return nil
	}
	self.msgLock.RUnlock()

	self.InsertMessage(message)

	var body map[string]any = map[string]any{}
	body["type"] = "broadcast"
	body["message"] = message
	for _, nbt := range self.otherNodesMetaInfo {
		err := node.Send(nbt.name, body)
		if err != nil {
			return err
		}
	}
	return nil
}

// func (self *NodeState) BackgroundSync() {
// 	for {
// 		time.Now()
// 		time.Sleep(1000)
// 		// if self.nodeId == "" {
// 		// 	continue
// 		// }
// 		nodesToSync := getRandomNodes(self.otherNodes)
// 		// log.Printf("nodesToSync: %v\n", nodesToSync)
//
// 		for _, nodeToSync := range nodesToSync {
// 			nodeMeta, exists := self.otherNodesMetaInfo[nodeToSync]
// 			if !exists {
// 				continue
// 			}
//
// 		}
//
// 	}
// }

func getRandomNodes(otherNodes []string) []string {
	randNodes := make([]string, 0, 2)
	n := len(otherNodes)
	for {
		len := len(randNodes)
		if len == 2 {
			return randNodes
		}
		node := otherNodes[rand.IntN(n)]
		if len == 0 {
			randNodes = append(randNodes, node)
		} else {
			if randNodes[0] == node {
				continue
			} else {
				randNodes = append(randNodes, node)
			}
		}
	}

}

// func (self *NodeState) ReadMessages(callback func(messages map[int]bool)) {
func (self *NodeState) ReadMessages(callback func(messages *MessageStore)) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messageStore)

}
