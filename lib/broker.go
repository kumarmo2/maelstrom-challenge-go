package lib

type Broker struct {
	logSyncChan chan *LogEvent
	logs        *ThreadSafeMap[string, *KafkaLog]
	ns          *NodeState
}

func NewBroker(logSyncChan chan *LogEvent, ns *NodeState) *Broker {
	return &Broker{logSyncChan: logSyncChan, logs: NewThreadSafeMap[string, *KafkaLog](), ns: ns}
}

func (self *Broker) Append(key string, msg any) int {
	logFunc := func() *KafkaLog {
		return NewLog(key, self.ns)
	}
	kafkaLog := self.logs.GetOrCreateAndThenGet(key, logFunc)
	return kafkaLog.Append(msg)
}

func (self *Broker) CommitOffset(key string, offset int) {
	logFunc := func() *KafkaLog {
		return NewLog(key, self.ns)
	}
	log := self.logs.GetOrCreateAndThenGet(key, logFunc)
	log.CommitOffset(offset)
}

func (self *Broker) GetCommitOffset(key string) int {
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
