package main

import (
	"log"

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

	q, err := ch.QueueDeclare(
		"test", // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		log.Println("ERROR failed to declare a queue:" + err.Error())
		return
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Println("ERROR failed to register a consumer:", err.Error())
		return
	}

	var forever chan struct{}

	go func() {
		for msg := range msgs {
			log.Println("RECEIVED :", string(msg.Body))
		}
	}()

	log.Println("Waiting For Messages, To EXIT press CTRL+C")
	<-forever

}
