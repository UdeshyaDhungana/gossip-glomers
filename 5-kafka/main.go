package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type sendReq struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type pollReq struct {
	Offsets map[string]int `json:"offsets"`
}

type commitReq struct {
	Offsets map[string]int `json:"offsets"`
}

type listCommittedReq struct {
	Keys []string `json:"keys"`
}

// KV layout:
//
//	"log_<key>"    -> []int  (message at index i has offset i)
//	"commit_<key>" -> int    (committed offset)
//	"lock_<key>"   -> string ("-1" when free, owning node ID when held)
func logKey(key string) string    { return "log_" + key }
func commitKey(key string) string { return "commit_" + key }
func lockKey(key string) string   { return "lock_" + key }

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	ctx := context.Background()

	// Same spin lock as 4-counter, but one lock per Kafka key so sends to
	// different keys don't block each other.
	lock := func(key string) {
		for {
			err := kv.CompareAndSwap(ctx, lockKey(key), "-1", n.ID(), true)
			if err == nil {
				return
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				log.Printf("lock acquire error (%s): %s", key, err)
			}
		}
	}

	unlock := func(key string) {
		if err := kv.CompareAndSwap(ctx, lockKey(key), n.ID(), "-1", false); err != nil {
			log.Printf("lock release error (%s): %s", key, err)
		}
	}

	// readLog returns the log for key, or an empty log if it doesn't exist yet.
	readLog := func(key string) ([]int, error) {
		var msgs []int
		err := kv.ReadInto(ctx, logKey(key), &msgs)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return nil, err
		}
		return msgs, nil
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var req sendReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		// This is a write operation: lock the db
		lock(req.Key)
		defer unlock(req.Key)

		// Read the log's value
		msgs, err := readLog(req.Key)
		if err != nil {
			return err
		}

		// Append new message
		msgs = append(msgs, req.Msg)

		// Set the log's value
		if err := kv.Write(ctx, logKey(req.Key), msgs); err != nil {
			return err
		}

		// Return the offset of the new message
		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": len(msgs) - 1,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var req pollReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		// No lock: lin-kv reads are linearizable and the log is append-only.
		responseMsgs := make(map[string][][2]int)
		for key, offset := range req.Offsets {
			msgs, err := readLog(key)
			if err != nil {
				return err
			}
			for i := offset; i < len(msgs); i++ {
				responseMsgs[key] = append(responseMsgs[key], [2]int{i, msgs[i]})
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": responseMsgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var req commitReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		for key, offset := range req.Offsets {
			if err := func() error {
				lock(key)
				defer unlock(key)

				msgs, err := readLog(key)
				if err != nil {
					return err
				}
				if offset > len(msgs)-1 {
					return nil
				}
				return kv.Write(ctx, commitKey(key), offset)
			}(); err != nil {
				return err
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var req listCommittedReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		// The KV store can't list keys, so only the requested keys are returned.
		v := make(map[string]int)
		for _, key := range req.Keys {
			offset, err := kv.ReadInt(ctx, commitKey(key))
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					continue
				}
				return err
			}
			v[key] = offset
		}

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": v,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
