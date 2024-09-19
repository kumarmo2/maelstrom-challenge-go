package lib

import "sync"

type NodeState struct {
	msgLock  *sync.RWMutex
	messages []int
}

func NewNodeState() *NodeState {
	return &NodeState{msgLock: &sync.RWMutex{}, messages: make([]int, 0)}
}

func (self *NodeState) InsertMessage(message int) {
	self.msgLock.Lock()
	defer self.msgLock.Unlock()
	self.messages = append(self.messages, message)
}

func (self *NodeState) ReadMessages(callback func(messages []int)) {
	self.msgLock.RLock()
	defer self.msgLock.RUnlock()
	callback(self.messages)

}
