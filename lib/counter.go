package lib

import (
	"sync"
	"time"
)

type GlobalGC[T any] struct {
	counter            int
	lock               *sync.RWMutex
	nodeState          *NodeState
	lastSyncFromStore  time.Time
	storeMessageNotify <-chan bool
}

func NewGlobalGC[T any](ns *NodeState, notification <-chan bool) *GlobalGC[T] {
	return &GlobalGC[T]{counter: 0, lock: &sync.RWMutex{}, nodeState: ns, lastSyncFromStore: time.Now(), storeMessageNotify: notification}
}

func (gc *GlobalGC[T]) Start() {
	go func() {
		for {
			_ = <-gc.storeMessageNotify

		}

	}()

}
