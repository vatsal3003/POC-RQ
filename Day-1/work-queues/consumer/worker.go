package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("ERROR failed to connect RabbitMQ:", err.Error())
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Println("ERROR failed to open channel:", err.Error())
		return
	}

	q, err := ch.QueueDeclare(
		"demoq", // name
		// false,   // durable
		true,  // durable  // make it durable so remaining tasks in queue will saved if rabbitmq server crashes
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Println("ERROR failed to declare a queue:", err.Error())
		return
	}

	// Disable Prefetch : In order to defeat that we can set the prefetch count with the value of 1. This tells RabbitMQ not to give more than one message to a worker at a time. Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one. Instead, it will dispatch it to the next worker that is not still busy.
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Println("ERROR failed to set QoS")
		return
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		// true,   // auto-ack
		false, // auto-ack // Will acknowledge manually
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Println("ERROR failed to register a consumer:", err.Error())
		return
	}

	var forever chan struct{}

	go func() {
		for msg := range msgs {
			log.Println("RECEIVED :", string(msg.Body))
			dotCount := bytes.Count(msg.Body, []byte("."))
			InitProgress(dotCount)
			log.Println("Processing Done!")

			msg.Ack(false) // If worker is terminated in between processing, It will not send acknowledgement so RabbitMQ will not marked this task for deletion, instead it will send to other consumer

			fmt.Println("-----------------------")
		}
	}()

	log.Println("Waiting for Messages, To EXIT press CTRL+C")
	<-forever
}

func InitProgress(dotCount int) {
	for range dotCount / 3 {
		for j := 1; j <= 3; j++ {
			PrintProcess(j)
		}
	}
	for i := range dotCount % 3 {
		PrintProcess(i + 1)
	}
	fmt.Println()
}

func PrintProcess(i int) {
	message := fmt.Sprintf("Processing%s", strings.Repeat(".", i))
	fmt.Printf("\r\x1b[K%s", message) // Overwrite the line with the new message
	time.Sleep(time.Second)
}
