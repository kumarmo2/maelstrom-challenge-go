package main

import (
	"encoding/json"
	"log"

	"github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	log.Println("hello world")
	node := maelstrom.NewNode()

	node.Handle("echo", handlerGenerator(node, handleEcho))

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}

}

func handlerGenerator(node *maelstrom.Node, h func(node *maelstrom.Node) maelstrom.HandlerFunc) func(msg maelstrom.Message) error {
	return h(node)
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
