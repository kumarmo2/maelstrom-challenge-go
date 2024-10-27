package lib

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

	self.otherNodes = make([]string, 0)

	for _, n := range allNodes {
		if n != self.nodeId {
			self.otherNodes = append(self.otherNodes, n)
		}
	}
	return self
}
