package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/kumarmo2/maelstrom-challenge-go/util"
)

type Broker struct {
	logs     *ThreadSafeMap[string, *KafkaLog]
	ns       *NodeState
	syncInfo *ThreadSafeMap[string, *logSyncInfo]
}

func NewBroker(ns *NodeState) *Broker {
	return &Broker{logs: NewThreadSafeMap[string, *KafkaLog](), ns: ns,
		syncInfo: NewThreadSafeMap[string, *logSyncInfo]()}
}

func generateFuncForKafkaLog(key string, ns *NodeState) func() *KafkaLog {
	return func() *KafkaLog {
		node := ns.node.ID()
		leaderKey := util.GetLogLeaderKey(key)
		err := ns.linKV.CompareAndSwap(context.Background(), leaderKey, node, node, true)
		var ownerNodeId string
		if err != nil {
			ownerNode, _ := ns.linKV.Read(context.Background(), leaderKey)
			ownerNodeId = ownerNode.(string)
			return NewLog(key, ns, ownerNodeId)
		}
		return NewLog(key, ns, node)

	}

}

func (self *Broker) HandleLogSyncAck(req *LogSyncAck) error {
	key := req.Key
	l, exists := self.logs.Get(key)
	if !exists {
		return errors.New(fmt.Sprintf("Broker.HandleLogSyncAck, node:%v doesn't contain log: %v", self.ns.node.ID(), key))
	}
	if !l.IsLeader() {
		return errors.New(fmt.Sprintf("Broker.HandleLogSyncAck, node:%v is not the leader of log: %v", self.ns.node.ID(), key))
	}
	return l.HandleLogSyncAck(req)
}

func (self *Broker) HandleLogEvent(event *LogEvent) error {
	if event == nil {
		e := fmt.Sprintf("HandleLogEvent: event was nil")
		log.Print(e)
		return errors.New(e)
	}
	key := event.Key
	l := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	if l.IsLeader() {
		e := fmt.Sprint("Broker.HandleLogEvent: node %v, is the leader of the log:%v ", self.ns.node.ID(), key)
		log.Print(e)
		return errors.New(e)
	}

	return l.HandleLogEvent(event)
}

func (self *Broker) Append(key string, msg any) int {
	kafkaLog := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	if kafkaLog.IsLeader() {
		logItem := kafkaLog.Append(msg)
		return logItem.Offset
	}
	ownerNode := kafkaLog.ownerNodeId
	body := SendRequestBody{
		Variant: "send",
		Key:     key,
		Msg:     msg,
	}
	ch := make(chan *SendResponseBody, 1)

	handler := func(msg maelstrom.Message) error {
		var body SendResponseBody
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			log.Fatalf("got error while unmarshalling response body: %v", err)
		}
		ch <- &body
		return nil
	}

	self.ns.node.RPC(ownerNode, body, handler)
	res := <-ch
	return res.Offset
}

func (self *Broker) CommitOffset(key string, offset int) {
	log := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	log.CommitOffset(offset)
}

func (self *Broker) GetCommitOffset(key string) int {
	otherNodesCounter := len(self.ns.otherNodes)
	if otherNodesCounter > 1 {

	}
	log := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	return log.GetCommitOffset()
}

func (self *Broker) GetAllFrom(key string, offset int) [][]any {
	log := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	return log.GetAllFrom(offset)
}
