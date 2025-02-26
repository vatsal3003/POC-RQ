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

	ch, err := conn.Channel()
	if err != nil {
		log.Println("ERROR failed to open channel:", err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"logs",   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		log.Println("ERROR failed to declare an exchange:", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var body = bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,
		"logs",                  // exchange
		checkTypeOfLog(os.Args), // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		log.Println("ERROR failed to publish a message:", err.Error())
		return
	}

	log.Println("SENT { body:", body, "}")
}

func bodyFrom(arr []string) string {
	var body string
	if len(arr) < 3 || arr[2] == "" {
		body = "hello"
	} else {
		body = strings.Join(arr[2:], " ")
	}
	return body
}

func checkTypeOfLog(arr []string) string {
	var logType string
	if len(arr) < 2 || arr[1] == "" {
		logType = "info"
	} else {
		logType = os.Args[1]
	}
	return logType
}
