package lib

import "sync"

type NodeState struct {
	msgLock  *sync.RWMutex
	messages map[int]bool
}

func NewNodeState() *NodeState {
	return &NodeState{msgLock: &sync.RWMutex{}, messages: make(map[int]bool)}
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	self.messages[message] = true
}

func (self *NodeState) ReadMessages(callback func(messages map[int]bool)) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messages)

}
