// Produce messages to Streamline using franz-go (standard Kafka client).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		msg, _ := json.Marshal(map[string]interface{}{"event": "test", "index": i})
		record := &kgo.Record{Topic: "demo", Value: msg}
		results := client.ProduceSync(ctx, record)
		if err := results.FirstErr(); err != nil {
			log.Fatal(err)
		}
		r := results[0].Record
		fmt.Printf("Sent to partition=%d offset=%d: %s\n", r.Partition, r.Offset, msg)
	}

	fmt.Println("Done! 5 messages produced to 'demo' topic.")
}
