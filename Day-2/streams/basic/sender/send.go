package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

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
		log.Println("ERROR failed to create environment:", err.Error())
		return
	}
	defer env.Close()

	streamName := "go-rmq-stream"
	env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	producer, err := env.NewProducer(streamName, stream.NewProducerOptions())
	if err != nil {
		log.Println("ERROR failed to create producer:", err.Error())
		return
	}

	message := strings.Join(os.Args[1:], " ")
	err = producer.Send(amqp.NewMessage([]byte(message)))
	if err != nil {
		log.Println("ERROR failed to send message:", err.Error())
		return
	}
	log.Println("Message sent:", message)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println(" [x] Press enter to close the producer")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	if err != nil {
		log.Println("ERROR failed to close producer: ", err.Error())
		return
	}
}
