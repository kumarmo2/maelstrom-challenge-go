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
	return message.num
}

type NodeState struct {
	msgLock    *sync.RWMutex
	messages   *AVLTree[*MessageItem]
	nodeId     string
	otherNodes map[string]*NodeMetaInfo
}

func NewNodeState(node *maelstrom.Node) *NodeState {
	self := &NodeState{msgLock: &sync.RWMutex{}, messages: NewAVLTRee[*MessageItem]()}
	self.nodeId = node.ID()
	allNodes := node.NodeIDs()

	self.otherNodes = make(map[string]*NodeMetaInfo)

	for _, n := range allNodes {
		if n != self.nodeId {
			self.otherNodes[n] = &NodeMetaInfo{name: n, lastSync: time.Now().Add(time.Minute)}
		}
	}
	return self
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	msg := newMessageItem(message)
	self.messages.InsertItem(msg)
}

func (self *NodeState) SaveBroadcastMessageIfNew(message int, node *maelstrom.Node) error {
	self.msgLock.RLock()
	exists := self.messages.ContainsKey(message)
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
	for _, nbt := range self.otherNodes {
		err := node.Send(nbt.name, body)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *NodeState) BackgroundSync() {
	for {
		time.Now()
		time.Sleep(700)
		if self.nodeId == "" {
			continue
		}
		// _ := getRandomNodes(self.otherNodes)

	}
}

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
func (self *NodeState) ReadMessages(callback func(messages *AVLTree[*MessageItem])) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messages)

}
