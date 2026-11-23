package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	var mu sync.Mutex
	deltas := []int{}

	flush := func() {
		for {
			err := kv.CompareAndSwap(ctx, "lock", "-1", n.ID(), true)
			if err == nil {
				break
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				log.Printf("lock acquire error: %s", err)
			}
		}

		// The test passes even without the lock but since handlers run in a separate go routine,
		// the lock is needed to protect deltas.
		mu.Lock()
		sum := 0
		for _, d := range deltas {
			sum += d
		}
		deltas = nil
		mu.Unlock()

		oldValue, err := kv.ReadInt(ctx, "counter")
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			log.Printf("counter read error: %s", err)
		} else {
			if err != nil {
				oldValue = 0
			}
			kv.CompareAndSwap(ctx, "counter", oldValue, oldValue+sum, true)
		}

		if err := kv.CompareAndSwap(ctx, "lock", n.ID(), "-1", false); err != nil {
			log.Printf("lock release error: %s", err)
		}
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			for {
				flush()
			}
		}()
		return nil
	})

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		value, err := kv.ReadInt(ctx, "counter")
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}
		body["value"] = value
		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		deltas = append(deltas, int(body["delta"].(float64)))
		mu.Unlock()

		body["type"] = "add_ok"
		delete(body, "delta")
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
