// Consume messages from Streamline using franz-go (standard Kafka client).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics("demo"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("Consuming from 'demo' topic (5s timeout)...")
	count := 0
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("  offset=%d partition=%d value=%s\n", r.Offset, r.Partition, r.Value)
			count++
		})
	}

	fmt.Printf("Done! Consumed %d messages.\n", count)
}
