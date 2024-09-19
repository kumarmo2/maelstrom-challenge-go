package main

import (
	"encoding/json"
	"log"

	// "math/rand"
	//
	// "time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/kumarmo2/maelstrom-challenge-go/lib"
)

var state *lib.NodeState = lib.NewNodeState()

func main() {
	log.Println("hello world")
	node := maelstrom.NewNode()

	node.Handle("echo", handlerGenerator(node, handleEcho))
	node.Handle("generate", handlerGenerator(node, handleGenerate))
	node.Handle("read", handlerGenerator(node, handleRead))
	node.Handle("broadcast", handlerGenerator(node, handleBroadcast))
	node.Handle("topology", handlerGenerator(node, handleTopology))

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}

}

func handlerGenerator(node *maelstrom.Node, h func(node *maelstrom.Node) maelstrom.HandlerFunc) maelstrom.HandlerFunc {
	return h(node)
}

func handleTopology(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		// TODO: handle the topology properly.
		body := make(map[string]any)
		body["type"] = "topology_ok"
		return node.Reply(msg, body)
	}
}

func handleRead(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var err error
		callback := func(messages map[int]bool) {
			var body map[string]any
			e := json.Unmarshal(msg.Body, &body)
			if e != nil {
				err = e
				return
			}
			body["type"] = "read_ok"

			msgs := make([]int, 0)
			for k := range messages {
				msgs = append(msgs, k)
			}

			body["messages"] = msgs
			err = node.Reply(msg, body)
		}
		state.ReadMessages(callback)
		return err
	}
}

func handleBroadcast(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		type Body struct {
			Variant string `json:"type"`
			Message int    `json:"message"`
		}
		var body Body

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		message := body.Message
		state.InsertMessage(int(message))

		var reply map[string]any = map[string]any{}
		reply["type"] = "broadcast_ok"
		return node.Reply(msg, reply)
	}

}

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
