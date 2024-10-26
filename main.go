package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/kumarmo2/maelstrom-challenge-go/lib"
)

var state *lib.NodeState
var broker *lib.Broker

func main() {
	node := maelstrom.NewNode()

	node.Handle("init", handlerGenerator(node, handleInit))
	node.Handle("echo", handlerGenerator(node, handleEcho))
	node.Handle("generate", handlerGenerator(node, handleGenerate))
	node.Handle("send", handlerGenerator(node, handleSend))
	node.Handle("log-event", handlerGenerator(node, handleKafkaLogEvent))
	node.Handle("log-sync-ack", handlerGenerator(node, handleKafkaLogSyncAck))
	node.Handle("poll", handlerGenerator(node, handlePoll))
	node.Handle("commit_offsets", handlerGenerator(node, handleCommitOffset))
	node.Handle("list_committed_offsets", handlerGenerator(node, handleListOffsets))
	node.Handle("get_committed_offset", handlerGenerator(node, handleGetOffset))
	node.Handle("broadcast_ok", handlerGenerator(node, noOp))
	node.Handle("topology", handlerGenerator(node, handleTopology))

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}

}

func noOp(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		return nil
	}
}

func handlerGenerator(node *maelstrom.Node, h func(node *maelstrom.Node) maelstrom.HandlerFunc) maelstrom.HandlerFunc {
	return h(node)
}

func handleKafkaLogSyncAck(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var event lib.CustomMessage[*lib.LogSyncAck]
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			return err

		}
		if err := broker.HandleLogSyncAck(event.Body); err != nil {
			return err
		}
		return node.Reply(msg, map[string]any{})
	}
}

func handleKafkaLogEvent(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var event lib.CustomMessage[*lib.LogEvent]
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			return err

		}
		return broker.HandleLogEvent(event.Body)
	}
}

func handleInit(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state = lib.NewNodeState(node)
		broker = lib.NewBroker(state)
		return nil
	}
}

func handleTopology(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		// TODO: handle the topology properly.
		body := make(map[string]any)
		body["type"] = "topology_ok"
		return node.Reply(msg, body)
	}
}

func handleGetOffset(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body *lib.CustomMessage[string]
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}
		key := body.Body
		offset := broker.GetCommitOffset(key)
		res := &lib.CustomMessage[int]{Body: offset, Type: "get_committed_offset_ok"}
		return node.Reply(msg, res)
	}
}

func handleListOffsets(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		type Body struct {
			Variant string   `json:"type"`
			Keys    []string `json:"keys"`
		}

		var body Body
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}
		result := lib.NewThreadSafeMap[string, int]()
		wg := &sync.WaitGroup{}

		for _, k := range body.Keys {
			wg.Add(1)
			go func() {
				offset := broker.GetCommitOffset(k)
				result.Set(k, offset)
				wg.Done()

			}()
		}
		wg.Wait()

		b := make(map[string]any)
		b["type"] = "list_committed_offsets_ok"
		b["offsets"] = result.ExposeInner()

		return node.Reply(msg, b)
	}
}

func handleCommitOffset(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		type Body struct {
			Variant string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}

		var body Body
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}
		wg := &sync.WaitGroup{}

		for k, v := range body.Offsets {
			wg.Add(1)
			go func() {
				broker.CommitOffset(k, v)
				wg.Done()
			}()
		}
		wg.Wait()

		result := make(map[string]any)
		result["type"] = "commit_offsets_ok"

		return node.Reply(msg, result)
	}
}

func handlePoll(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		type Body struct {
			Variant string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}

		var body Body
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}

		result := make(map[string][][]any)

		for k, v := range body.Offsets {
			items := broker.GetAllFrom(k, v)
			// NOTE: we should be able to parallelize this.
			result[k] = items
		}

		b := make(map[string]any)
		b["type"] = "poll_ok"
		b["msgs"] = result
		return node.Reply(msg, b)
	}
}

func handleSend(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body lib.SendRequestBody
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}

		offset := broker.Append(body.Key, body.Msg)

		var reply map[string]any = map[string]any{}
		reply["type"] = "send_ok"
		reply["offset"] = offset
		return node.Reply(msg, reply)
	}
}

// func handleBroadcast(node *maelstrom.Node) maelstrom.HandlerFunc {
// 	return func(msg maelstrom.Message) error {
// 		type Body struct {
// 			Variant string       `json:"type"`
// 			Message lib.LogEvent `json:"message"`
// 		}
// 		var body Body
//
// 		err := json.Unmarshal(msg.Body, &body)
// 		if err != nil {
// 			return err
// 		}
//
// 		message := &body.Message
// 		state.InsertMessage(message)
// 		var reply map[string]any = map[string]any{}
// 		reply["type"] = "broadcast_ok"
// 		return node.Reply(msg, reply)
// 	}
// }

func handleGenerate(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		id := uuid.New()

		body["type"] = "generate_ok"
		body["id"] = id
		return node.Reply(msg, body)
	}
}

func handleEcho(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {

		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return nil
		}
		body["type"] = "echo_ok"
		return node.Reply(msg, body)

	}

}
