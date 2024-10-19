package lib

import (
	"context"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type GlobalGC struct {
	Counter           int
	Lock              *sync.RWMutex
	nodeState         *NodeState
	lastSyncFromStore time.Time
	NotifyChan        chan bool
	seqKV             *maelstrom.KV
}

func NewGlobalGC(ns *NodeState, notification chan bool, seqKV *maelstrom.KV) *GlobalGC {
	return &GlobalGC{Counter: 0, Lock: &sync.RWMutex{}, nodeState: ns, lastSyncFromStore: time.Now(), NotifyChan: notification, seqKV: seqKV}
}

func (gc *GlobalGC) Start() {
	go func() {
		for {
			_ = <-gc.nodeState.notifyWhenNewMsgs
			msgs := gc.nodeState.GetItemsGreaterThan(gc.lastSyncFromStore)
			n := len(msgs)
			if n == 0 {
				continue
			}
			gc.Lock.Lock()
			prevVal := gc.Counter
			gc.updateCounter(msgs, prevVal)
			log.Println("==== doing CAS from gc")
			err := gc.seqKV.CompareAndSwap(context.Background(), "counter", prevVal, gc.Counter, true)
			gc.Lock.Unlock()

			if err != nil {
				log.Println("GC sync: sync to kv store failed, that means we don't have the latest value")
				continue
			} else {
				log.Println("GC sync: sync to kv store successfull")

			}

			select {
			case gc.NotifyChan <- true:
			default:
				log.Println("gc.NotifyChan: could'nt send")
			}
		}

	}()
}

func (gc *GlobalGC) updateCounter(msgs []*MessageItem[int], oldVal int) {

	for _, v := range msgs {
		if v.Time.UnixMilli() > gc.lastSyncFromStore.UnixMilli() {
			gc.lastSyncFromStore = v.Time
			gc.Counter += v.Message
		}
	}

}
