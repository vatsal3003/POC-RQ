package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vatsal3003/poc-rq/pkg/models"
)

type RabbitMQClient struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

func NewRabbitMQClient(url, queueName string) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &RabbitMQClient{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
	}, nil
}

func (c *RabbitMQClient) Close() {
	if c.channel != nil {
		c.channel.Close()
	}

	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *RabbitMQClient) PublishJob(job models.ResizeJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = c.channel.PublishWithContext(
		ctx,
		"",          // exchange
		c.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}

	return nil
}

func (c *RabbitMQClient) ConsumeJobs(processFunc func(models.ResizeJob) error, stopChan <-chan struct{}) error {
	err := c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := c.channel.Consume(
		c.queueName, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	forever := make(chan struct{})

	go func() {
		for {
			select {
			case <-stopChan:
				log.Println("Worker shutting down")
				return
			case d, ok := <-msgs:
				if !ok {
					log.Println("Channel closed")
					return
				}

				log.Println("Received a message:", d.Body)

				var job models.ResizeJob
				err := json.Unmarshal(d.Body, &job)
				if err != nil {
					log.Println("ERROR unmarshaling job:", err.Error())
					d.Ack(false)
					continue
				}

				err = processFunc(job)
				if err != nil {
					log.Printf("Error processing job %s: %s", job.JobID, err)
					// TODO: can implement DLQ here
					d.Ack(false)
					continue
				}

				log.Println("successfully processed job", job.JobID)
				d.Ack(false)
			}
		}
	}()

	log.Println("Worker started")
	log.Println("Waiting for messages")

	<-forever
	return nil
}
