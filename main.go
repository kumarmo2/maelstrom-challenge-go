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

func main() {
	log.Println("hello world")
	node := maelstrom.NewNode()

	node.Handle("init", handlerGenerator(node, handleInit))
	node.Handle("gossip-send-data", handlerGenerator(node, handleGossipSendData))
	node.Handle("gossip-send-data-ack", handlerGenerator(node, handleGossipSendDataAck))
	node.Handle("echo", handlerGenerator(node, handleEcho))
	node.Handle("generate", handlerGenerator(node, handleGenerate))
	node.Handle("read", handlerGenerator(node, handleRead))
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
		// Note: I'am assuming that the 'init' will be called first and just once.
		state = lib.NewNodeState(node)
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

func handleRead(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var err error
		callback := func(store *lib.MessageStoreV2[int]) {
			var body map[string]any
			e := json.Unmarshal(msg.Body, &body)
			if e != nil {
				err = e
				return
			}
			body["type"] = "read_ok"

			// msgs := util.ToValSlice(store.MessageMap)

			n := len(store.MessageMap)
			nums := make([]int, n)

			i := 0

			for _, msg := range store.MessageMap {
				nums[i] = msg.Message
				i++
			}

			body["messages"] = nums
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
