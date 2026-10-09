package cacheq_test

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// ExampleClient_SubscribeEvents observes a first load, cache reuse, and client shutdown for one key.
func ExampleClient_SubscribeEvents() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	events, err := client.SubscribeEvents(cacheq.EventOptions{Key: "product:42", Buffer: 16})
	if err != nil {
		panic(err)
	}
	defer events.Close()
	load := func(context.Context) (string, error) { return "product 42", nil }
	for range 2 {
		if _, err := cacheq.Fetch(context.Background(), client, "product:42", load); err != nil {
			panic(err)
		}
	}

	// This short example fits in the buffer; a running service should consume concurrently.
	client.Close()
	for event := range events.Events() {
		fmt.Printf("%v %s load=%d source=%s reason=%s joined=%d attempts=%d\n",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason, event.Joined, event.Attempts)
	}
	fmt.Printf("dropped=%d\n", events.Dropped())

	// Output:
	// product:42 load_started load=1 source=fetch reason=missing joined=0 attempts=0
	// product:42 load_finished load=1 source=fetch reason=missing joined=0 attempts=1
	// product:42 cache_hit load=0 source=fetch reason=fresh joined=0 attempts=0
	// <nil> client_closed load=0 source=none reason=client_closed joined=0 attempts=0
	// dropped=0
}
