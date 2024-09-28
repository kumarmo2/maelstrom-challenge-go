package lib

import (
	"math/rand/v2"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type MessageItem struct {
	num int
}

type NodeState struct {
	msgLock    *sync.RWMutex
	messages   map[int]bool
	nodeId     string
	otherNodes []string
}

func NewNodeState() *NodeState {
	return &NodeState{msgLock: &sync.RWMutex{}, messages: make(map[int]bool)}
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	self.messages[message] = true
}

func (self *NodeState) setNodesInfo(node *maelstrom.Node) {
	self.nodeId = node.ID()
	allNodes := node.NodeIDs()

	self.otherNodes = make([]string, 0)

	for _, n := range allNodes {
		if n != self.nodeId {
			self.otherNodes = append(self.otherNodes, n)
		}
	}
}

func (self *NodeState) SaveBroadcastMessageIfNew(message int, node *maelstrom.Node) error {
	sync.OnceFunc(func() { self.setNodesInfo(node) })()
	// fmt.Fprintf(os.Stderr, "otherNodes: %v", otherNodes)
	self.msgLock.RLock()
	_, exists := self.messages[message]
	if exists {
		// No need to broadcast further.
		self.msgLock.RUnlock()
		return nil
	}
	self.msgLock.RUnlock()

	self.InsertMessage(message)
	// self.msgLock.Lock()
	//
	// self.messages[message] = true
	//
	// self.msgLock.Unlock()

	// nodesToBroadCastTo := getRandomNodes(otherNodes)
	var body map[string]any = map[string]any{}
	body["type"] = "broadcast"
	body["message"] = message
	for _, nbt := range self.otherNodes {
		err := node.Send(nbt, body)
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

func (self *NodeState) ReadMessages(callback func(messages map[int]bool)) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messages)

}
