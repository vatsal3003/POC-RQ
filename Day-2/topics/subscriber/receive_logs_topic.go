package main

import (
	"log"
	"os"

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
		"logs",  // name
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Println("ERROR failed to declare an exchange:", err.Error())
		return
	}

	// The name will generate randomly,
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Println("ERROR failed to declare a Queue:", err.Error())
		return
	}

	if len(os.Args) < 2 {
		log.Println("Use:", os.Args[0], "[binding_key]...")
		return
	}
	for _, routeKey := range os.Args[1:] {
		log.Println("Binding Queue", q.Name, "to exchange", "logs", "with Routing Key", routeKey)
		err = ch.QueueBind(
			q.Name,   // queue name
			routeKey, // routing key
			"logs",   // exchange
			false,
			nil,
		)
		if err != nil {
			log.Println("ERROR failed to bind a queue:", err.Error())
			return
		}
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

	log.Println("Waiting For Logs, To EXIT press CTRL+C")
	<-forever
}
