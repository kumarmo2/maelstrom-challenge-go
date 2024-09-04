package main

import (
	"encoding/json"
	"log"
	// "math/rand"
	//
	// "time"

	"github.com/google/uuid"
	"github.com/jepsen-io/maelstrom/demo/go"
	// crate "github.com/kumarmo2/maelstrom-challenge-go/maelstromchallengego"
	// ulid "github.com/oklog/ulid/v2"
)

func main() {
	log.Println("hello world")
	node := maelstrom.NewNode()

	node.Handle("echo", handlerGenerator(node, handleEcho))
	node.Handle("generate", handlerGenerator(node, handleGenerate))

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}

}

func handlerGenerator(node *maelstrom.Node, h func(node *maelstrom.Node) maelstrom.HandlerFunc) maelstrom.HandlerFunc {
	return h(node)
}

func handleGenerate(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		// var entropy = crate.NewRand()
		// entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		// id, err := ulid.New(ulid.Timestamp(time.Now()), entropy)
		// if err != nil {
		// 	return err
		// }
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
