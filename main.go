package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/kumarmo2/maelstrom-challenge-go/lib"
)

var state *lib.NodeState
var gc *lib.GlobalGCV2

func main() {
	log.Println("hello world")
	node := maelstrom.NewNode()

	node.Handle("init", handlerGenerator(node, handleInit))
	node.Handle("gossip-send-data", handlerGenerator(node, handleGossipSendData))
	node.Handle("gossip-send-data-ack", handlerGenerator(node, handleGossipSendDataAck))
	node.Handle("echo", handlerGenerator(node, handleEcho))
	node.Handle("generate", handlerGenerator(node, handleGenerate))
	node.Handle("read", handlerGenerator(node, handleRead))
	node.Handle("add", handlerGenerator(node, handleAdd))
	node.Handle("broadcast", handlerGenerator(node, handleBroadcast))
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

func handleGossipSendDataAck(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {

		var body lib.GossipSendDataAck

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		src := msg.Src
		state.NodesMetaInfoLock.Lock()
		defer state.NodesMetaInfoLock.Unlock()

		srcNodeMeta, exists := state.OtherNodesMetaInfo[src]
		if !exists {
			return errors.New(fmt.Sprintf("src node: %v, does not exists", src))
		}
		if body.LastSync.UnixMilli() > srcNodeMeta.LastSync.UnixMilli() {
			srcNodeMeta.LastSync = body.LastSync
		}
		return node.Reply(msg, map[string]any{})
	}
}
func handleGossipSendData(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body lib.GossipSendData[int]
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return nil
		}

		if body.Messages == nil {
			log.Println("in handleGossipSendData, received null messages")
			return nil
		}

		if len(body.Messages) == 0 {
			log.Println("handleGossipSendData: zero messages found, returning")
			return nil
		}

		// NOTE: ideally these messages should be ordered by time

		lastSync, err := state.InsertMessageItems(body.Messages)
		if err != nil {
			log.Printf("error: %v\n", err)
			return err
		}
		response := lib.GossipSendDataAck{LastSync: lastSync, Type: "gossip-send-data-ack"}

		return node.RPC(msg.Src, response, func(msg maelstrom.Message) error { return nil })
	}
}

func handleInit(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body maelstrom.InitMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state = lib.NewNodeState(node)
		gc = lib.NewGlobalGCV2(state)
		gc.Start()

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
func handleAdd(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		type Body struct {
			Variant string `json:"type"`
			Val     int    `json:"delta"`
		}
		var body Body
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}
		val := body.Val
		log.Printf("got %v to add: ", val)
		state.InsertMessage(val)
		var reply map[string]any = map[string]any{}
		reply["type"] = "add_ok"
		return node.Reply(msg, reply)

	}
}

func handleRead(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body map[string]any
		e := json.Unmarshal(msg.Body, &body)
		if e != nil {
			return e
		}

		val := gc.Counter
		body["type"] = "read_ok"
		body["value"] = val
		return node.Reply(msg, body)

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
		state.InsertMessage(message)
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
