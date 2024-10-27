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
	logs     *NicheThreadSafeMap[string, *KafkaLog]
	ns       *NodeState
	syncInfo *NicheThreadSafeMap[string, *logSyncInfo]
}

func NewBroker(ns *NodeState) *Broker {
	return &Broker{logs: NewNicheThreadSafeMap[string, *KafkaLog](), ns: ns,
		syncInfo: NewNicheThreadSafeMap[string, *logSyncInfo]()}
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
	l := self.logs.GetOrCreateAndThenGet(key, nil) //NOTE: we are passing the nil generator because, this is a method for acknowledging
	// That should mean that log must have "gossiped" something
	if !l.IsLeader() {
		return errors.New(fmt.Sprintf("Broker.HandleLogSyncAck, node:%v is not the leader of log: %v", self.ns.node.ID(), key))
	}
	return l.HandleLogSyncAck(req)
}

func (self *Broker) HandleLogEvent(event *LogEvent) error {
	if event == nil {
		e := fmt.Sprintf("HandleLogEvent: event was nil")
		// log.Print(e)
		return errors.New(e)
	}
	key := event.Key
	l := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	if l.IsLeader() {
		e := fmt.Sprint("Broker.HandleLogEvent: node %v, is the leader of the log:%v ", self.ns.node.ID(), key)
		// log.Print(e)
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

	if log.IsLeader() {
		log.CommitOffset(offset)
	} else {
		var b *CustomMessage[map[string]int] = &CustomMessage[map[string]int]{
			Type: "commit_offsets",
			Body: map[string]int{key: offset}}

		ch := make(chan bool, 1)
		handler := func(msg maelstrom.Message) error {
			ch <- true
			return nil
		}
		self.ns.node.RPC(log.ownerNodeId, b, handler)
		<-ch
	}
}

func (self *Broker) GetCommitOffset(key string) int {
	log := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	if log.IsLeader() {
		return log.GetCommitOffset()
	} else {
		var b *CustomMessage[string] = &CustomMessage[string]{Type: "get_committed_offset", Body: key}
		ch := make(chan int, 1)
		handler := func(msg maelstrom.Message) error {
			var body CustomMessage[int]
			err := json.Unmarshal(msg.Body, &body)
			if err != nil {
				return err
			}
			ch <- body.Body
			return nil
		}
		self.ns.node.RPC(log.ownerNodeId, b, handler)
		return <-ch
	}
}

func (self *Broker) GetAllFrom(key string, offset int) [][]any {
	log := self.logs.GetOrCreateAndThenGet(key, generateFuncForKafkaLog(key, self.ns))
	return log.GetAllFrom(offset)
}
