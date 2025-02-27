package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetHost("localhost").
		SetPort(5552).
		SetUser("guest").
		SetPassword("guest"))
	if err != nil {
		log.Println("Failed to create environment:", err.Error())
		return
	}

	streamName := "go-rmq-stream"
	env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	messagesHandler := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("Stream: %s - Received message: %s\n", consumerContext.Consumer.GetStreamName(),
			message.Data)
	}

	consumer, err := env.NewConsumer(streamName, messagesHandler,
		stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()))
	if err != nil {
		log.Println("ERROR failed to create consumer:", err.Error())
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println(" [x] Waiting for messages. enter to close the consumer")
	_, _ = reader.ReadString('\n')
	err = consumer.Close()
	if err != nil {
		log.Println("ERROR failed to close consumer: ", err.Error())
		return
	}
}
