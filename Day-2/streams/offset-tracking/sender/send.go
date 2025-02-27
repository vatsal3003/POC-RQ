package main

import (
	"fmt"
	"log"

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

	producer, err := env.NewProducer(streamName, stream.NewProducerOptions())
	if err != nil {
		log.Println("ERROR failed to create producer:", err.Error())
		return
	}

	messageCount := 100
	ch := make(chan struct{})
	chPublishConfirm := producer.NotifyPublishConfirmation()

	handlePublishConfirm(chPublishConfirm, messageCount, ch)

	fmt.Println("Publishing", messageCount, "messages")
	for i := 0; i < messageCount; i++ {
		var body string
		if i == messageCount-1 {
			body = "marker"
		} else {
			body = "hello"
		}
		err = producer.Send(amqp.NewMessage([]byte(body)))
		if err != nil {
			log.Println("ERROR failed to send message:", err.Error())
			return
		}
	}

	<-ch
	fmt.Println("Messages confirmed")

	err = producer.Close()
	if err != nil {
		log.Println("ERROR failed to close producer:", err.Error())
		return
	}
}

func handlePublishConfirm(confirms stream.ChannelPublishConfirm, messageCount int, ch chan struct{}) {
	go func() {
		confirmedCount := 0
		for confirmed := range confirms {
			for _, msg := range confirmed {
				if msg.IsConfirmed() {
					confirmedCount++
					if confirmedCount == messageCount {
						ch <- struct{}{}
					}
				}
			}
		}
	}()
}
