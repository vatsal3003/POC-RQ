package main

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	if err != nil {
		log.Println("ERROR failed to create new stream environment:", err.Error())
		return
	}
	defer env.Close()

	streamName := "offset-tracking-stream-go"
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)
	if err != nil {
		log.Println("ERROR failed to declare stream:", err.Error())
		return
	}

	var firstOffset int64 = -1
	var messageCount int64 = -1
	var lastOffset atomic.Int64
	ch := make(chan struct{})
	messagesHandler := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		if atomic.CompareAndSwapInt64(&firstOffset, -1, consumerContext.Consumer.GetOffset()) {
			fmt.Println("First message received.")
		}
		if atomic.AddInt64(&messageCount, 1)%10 == 0 {
			_ = consumerContext.Consumer.StoreOffset()
		}
		if string(message.GetData()) == "marker" {
			lastOffset.Store(consumerContext.Consumer.GetOffset())
			_ = consumerContext.Consumer.StoreOffset()
			_ = consumerContext.Consumer.Close()
			ch <- struct{}{}
		}
	}

	var offsetSpecification stream.OffsetSpecification
	consumerName := "offset-tracking-go"
	storedOffset, err := env.QueryOffset(consumerName, streamName)
	if errors.Is(err, stream.OffsetNotFoundError) {
		offsetSpecification = stream.OffsetSpecification{}.Next()
	} else {
		offsetSpecification = stream.OffsetSpecification{}.Offset(storedOffset + 1)
	}

	_, err = env.NewConsumer(streamName, messagesHandler,
		stream.NewConsumerOptions().
			SetManualCommit().
			SetConsumerName(consumerName).
			SetOffset(offsetSpecification))
	if err != nil {
		log.Println("ERROR failed to make new consumer:", err.Error())
		return
	}
	fmt.Println("Started consuming...")

	<-ch
	fmt.Printf("Done consuming, first offset %d, last offset %d.\n", firstOffset, lastOffset.Load())
}
