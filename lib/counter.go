package lib

type GlobalGCV2 struct {
	Counter   int
	nodeState *NodeState
}

func NewGlobalGCV2(ns *NodeState) *GlobalGCV2 {
	return &GlobalGCV2{Counter: 0, nodeState: ns}
}

func (gc *GlobalGCV2) Start() {
	go func() {
		for {
			val := <-gc.nodeState.notifyWhenNewMsgs
			gc.Counter += val
		}

	}()
}
