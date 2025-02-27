package main

import (
	"context"
	"log"
	"strconv"
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
		"rpc_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Println("ERROR failed to declare a queue:", err.Error())
		return
	}

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Println("ERROR failed to set QoS:", err.Error())
		return
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args	)
	)
	if err != nil {
		log.Println("ERROR failed register a consumer:", err.Error())
		return
	}

	var forever chan struct{}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for msg := range msgs {
			n, err := strconv.Atoi(string(msg.Body))
			if err != nil {
				log.Println("ERROR failed to convert int to string:", err.Error())
				return
			}
			log.Printf("fibonacci(%d)\n", n)
			resp := fibonacci(n)

			err = ch.PublishWithContext(
				ctx,
				"",          // exchange
				msg.ReplyTo, // routing key
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: msg.CorrelationId,
					Body:          []byte(strconv.Itoa(resp)),
				})
			if err != nil {
				log.Println("ERROR failed to publish a message:", err.Error())
				return
			}

			msg.Ack(false)
		}
	}()

	log.Println("Awaiting RPC Requests")
	<-forever
}

func fibonacci(n int) int {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	} else {
		return fibonacci(n-1) + fibonacci(n-2)
	}
}
