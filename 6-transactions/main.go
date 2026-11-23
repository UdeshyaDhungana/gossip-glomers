package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// opType is the kind of a txn operation: "r" or "w" on the wire.
type opType uint8

const (
	opRead  opType = iota // "r"
	opWrite               // "w"
)

func (t opType) String() string {
	switch t {
	case opRead:
		return "r"
	case opWrite:
		return "w"
	default:
		return fmt.Sprintf("opType(%d)", uint8(t))
	}
}

func (t opType) MarshalJSON() ([]byte, error) {
	if t != opRead && t != opWrite {
		return nil, fmt.Errorf("txn op: unknown type %d", uint8(t))
	}
	return json.Marshal(t.String())
}

func (t *opType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "r":
		*t = opRead
	case "w":
		*t = opWrite
	default:
		return fmt.Errorf("unknown value %q, want \"r\" or \"w\"", s)
	}
	return nil
}

// txnOp is a single operation, sent on the wire as a 3-element array:
// ["r", 1, null] or ["w", 1, 6]. Val is nil for a read's null.
type txnOp struct {
	Type opType
	Key  int
	Val  *int
}

func (op *txnOp) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 3 {
		return fmt.Errorf("txn op: expected 3 elements, got %d", len(raw))
	}
	if err := json.Unmarshal(raw[0], &op.Type); err != nil {
		return fmt.Errorf("txn op type: %w", err)
	}
	if err := json.Unmarshal(raw[1], &op.Key); err != nil {
		return fmt.Errorf("txn op key: %w", err)
	}
	// null leaves Val as nil.
	if err := json.Unmarshal(raw[2], &op.Val); err != nil {
		return fmt.Errorf("txn op value: %w", err)
	}
	return nil
}

// MarshalJSON writes the op back as an array, so the reply's "txn" has the
// same shape as the request.
func (op txnOp) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{op.Type, op.Key, op.Val})
}

type txnRequest struct {
	Txn []txnOp `json:"txn"`
}

// entry is a key's value plus its version. The same shape is stored locally
// and in the central store, so versions can be compared.
type entry struct {
	Value   int `json:"value"`
	Version int `json:"version"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	ctx := context.Background()

	var mu sync.Mutex
	local := make(map[int]entry)

	lock := func(key int) {
		lockKey := "lock_" + strconv.Itoa(key)
		for {
			err := kv.CompareAndSwap(ctx, lockKey, "-1", n.ID(), true)
			if err == nil {
				return
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				log.Printf("lock acquire error (%d): %s", key, err)
			}
		}
	}
	unlock := func(key int) {
		lockKey := "lock_" + strconv.Itoa(key)
		if err := kv.CompareAndSwap(ctx, lockKey, n.ID(), "-1", false); err != nil {
			log.Printf("lock release error (%d): %s", key, err)
		}
	}

	pull := func(key int, central entry) {
		mu.Lock()
		defer mu.Unlock()
		if central.Version > local[key].Version {
			local[key] = central
		}
	}

	syncKey := func(key int, e entry) error {
		lock(key)
		defer unlock(key)

		versionKey := "version_" + strconv.Itoa(key)
		centralVersion, err := kv.ReadInt(ctx, versionKey)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				centralVersion = 0
			} else {
				return err
			}
		}
		if centralVersion < e.Version {
			if err := kv.Write(ctx, versionKey, e.Version); err != nil {
				return err
			}
			if err := kv.Write(ctx, strconv.Itoa(key), e.Value); err != nil {
				return err
			}
		} else {
			pull(key, entry{Value: centralVersion, Version: e.Version})
		}
		return nil
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			for {
				time.Sleep(100 * time.Millisecond)

				// Copy so the local lock isn't held across network calls.
				mu.Lock()
				snapshot := maps.Clone(local)
				mu.Unlock()

				for key, e := range snapshot {
					if err := syncKey(key, e); err != nil {
						log.Printf("sync error (%d): %s", key, err)
					}
				}
			}
		}()
		return nil
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		var req txnRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return fmt.Errorf("txn: %w", err)
		}

		// Guards the local map against the sync goroutine and other txns.
		mu.Lock()
		for i := range req.Txn {
			op := &req.Txn[i] // pointer, so changes land in req.Txn
			switch op.Type {
			case opRead:
				// A key that was never written reads as null, so Val stays nil.
				if e, ok := local[op.Key]; ok {
					val := e.Value
					op.Val = &val
				}
			case opWrite:
				e := local[op.Key]
				local[op.Key] = entry{Value: *op.Val, Version: e.Version + 1}
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  req.Txn,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
