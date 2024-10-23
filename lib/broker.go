package lib

import ()

type logSyncInfo struct {
	node           string
	lastSyncOffset int
}

type Broker struct {
	// logSyncChan chan *LogEvent
	logs     *ThreadSafeMap[string, *KafkaLog]
	ns       *NodeState
	syncInfo *ThreadSafeMap[string, *logSyncInfo]
}

func NewBroker(ns *NodeState) *Broker {
	return &Broker{logs: NewThreadSafeMap[string, *KafkaLog](), ns: ns,
		syncInfo: NewThreadSafeMap[string, *logSyncInfo]()}
}

func (self *Broker) BackgroundSync() {
	// go func() {
	// 	for {
	// 		time.Sleep(10 * time.Millisecond)
	// 		for _, otherNode := range self.ns.otherNodes {
	// 			self.syncInfo.GetOrCreateAndThenGet(otherNode, generateFuncForLogSyncInfo())
	// 		}
	// 	}
	//
	// }()

}

//
// func (self *Broker) Start() {
// 	go func() {
// 		for {
// 			event := <-self.logSyncChan
// 			if event == nil {
// 				log.Printf("log event was nil")
// 				continue
// 			}
// 			key := event.key
// 			logFunc := func() *KafkaLog {
// 				return NewLog(key, self.ns)
// 			}
//
// 			l := self.logs.GetOrCreateAndThenGet(key, logFunc)
// 			l.SyncLogItem(event.item)
// 		}
//
// 	}()
//
// }

func (self *Broker) Append(key string, msg any) int {
	logFunc := func() *KafkaLog {
		return NewLog(key, self.ns)
	}
	kafkaLog := self.logs.GetOrCreateAndThenGet(key, logFunc)
	logItem := kafkaLog.Append(msg)
	// event := &LogEvent{key: key, item: logItem}

	// self.ns.InsertMessageWithoutSendingEvent(event)
	return logItem.offset
}

func (self *Broker) CommitOffset(key string, offset int) {
	logFunc := func() *KafkaLog {
		return NewLog(key, self.ns)
	}
	log := self.logs.GetOrCreateAndThenGet(key, logFunc)
	log.CommitOffset(offset)
}

func (self *Broker) GetCommitOffset(key string) int {
	otherNodesCounter := len(self.ns.otherNodes)
	if otherNodesCounter > 1 {

	}
	// self.ns.node.RPC()
	logFunc := func() *KafkaLog {
		return NewLog(key, self.ns)
	}
	log := self.logs.GetOrCreateAndThenGet(key, logFunc)
	return log.GetCommitOffset()
}

func (self *Broker) GetAllFrom(key string, offset int) [][]any {
	logFunc := func() *KafkaLog {
		return NewLog(key, self.ns)
	}
	log := self.logs.GetOrCreateAndThenGet(key, logFunc)
	return log.GetAllFrom(offset)
}
