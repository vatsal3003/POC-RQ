package main

import (
	"context"
	"log"
	"os"
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
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Println("ERROR failed to open channel:", err.Error())
		return
	}
	defer ch.Close()

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var body string
	if len(os.Args) < 2 || os.Args[1] == "" {
		body = "hello"
	} else {
		body = strings.Join(os.Args[1:], " ")
	}

	err = ch.PublishWithContext(
		ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Making Messages as Persistent
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		log.Println("ERROR failed to publish a message:", err.Error())
		return
	}

	log.Println("SENT { body:", body, "}")
}
