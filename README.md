# Kinesumer

Kinesumer is a Go client implementing a client-side distributed consumer group client for [Amazon Kinesis](https://aws.amazon.com/kinesis/). It supports following features:

- Implement the client-side distributed Kinesis consumer group client.
- A client can consume messages from multiple Kinesis streams.
- Clients are automatically assigned a shard id range for each stream.
- Rebalance each shard id range when clients or upstream shards are changed. (by restart or scaling issues)
- Manage the checkpoint for each shard, so that clients can continue to consume from the last checkpoints.
- Able to consume from the Kinesis stream in a different AWS account.
- Manage all the consumer group client states with a DynamoDB table. (we call this table as `state store`.)

![architecture](./docs/images/architecture.png)

## Setup

Kinesumer manages the state of the distributed clients with a database, called "state store". It uses the DynamoDB as the state store, so you need to create a DynamoDB table first. Create a table with [LSI schema](./schema/ddb-lsi.json). See the details in [here](#how-it-works).

## Usage

```go
package main

import (
  "fmt"
  "time"
  
  "github.com/daangn/kinesumer"
)

func main() {
  client, err := kinesumer.NewKinesumer(
    &kinesumer.Config{
      App:              "myapp",
      KinesisRegion:    "ap-northeast-2",
      DynamoDBRegion:   "ap-northeast-2",
      DynamoDBTable:    "kinesumer-state-store",
      ScanLimit:        1500,
      ScanTimeout:      2 * time.Second,
  	},
  )
  if err != nil {
    // Error handling.
  }
  
  go func() {
    for err := range client.Errors() {
      // Error handling.
    }
  }()
  
  // Consume multiple streams.
  // You can refresh the streams with `client.Refresh()` method.
  records, err := client.Consume([]string{"stream1", "stream2"})
	if err != nil {
		// Error handling.
	}

	for record := range records {
		fmt.Printf("record: %v\n", record)
	}
}
	
```

## How it works

Kinesumer implements the client-side distributed consumer group client without any communications between nodes (refre to consumer group clients). Then, how do clients know the state of an entire system? The answer is the centralized database. In order for this system to work well, the Kinesumer uses a database to manage the states of the distributed clients, shard cache, and checkpoints, called `state store`. 

This picture describes the overview architecture of Kinesumer:

![how-it-works](./docs/images/how-it-works.png)

Following describes the process of how the Kinesumer works:

- Clients register themselves to the state store and determine the leader client. The leader will be determined by the index of the list sorted by client id. And, zero indexes will be a leader.
  - When rebalancing occurs, the leader could be changed.
- All clients periodically ping the state store to indicate that they are active.
- A client will fetch the full shard id list and client list from the state store. Then, divide the shard id list by the number of clients and assign a range of shard id corresponding to their index.
- All clients including leader will repeat the above process. (so, we will be able to do automatic rebalancing for free.)
- The leader client does more things than follower clients. It is responsible to sync the shard cache with the latest value, and pruning the outdated client list (to prevent the orphan shard range) periodically.
- Whenever a client consumes messages from its assigned shards, it updates a per-shard checkpoint with the sequence number of the last message read from each shard.

## License

See [LICENSE](./LICENSE).

