package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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
		if !strings.Contains(err.Error(), "406") { // ignore error if error is queue is already there
			ch.Close()
			conn.Close()
			return nil, fmt.Errorf("failed to declare a queue: %w", err)
		}
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
	// Set up Dead Letter Exchange (DLX) and Queue
	dlxName := c.queueName + ".dlx"
	dlqName := c.queueName + ".dlq"

	// Declare the Dead Letter Exchange
	err := c.channel.ExchangeDeclare(
		dlxName,  // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare DL exchange: %w", err)
	}

	// Declare the Dead Letter Queue
	_, err = c.channel.QueueDeclare(
		dlqName, // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare DL queue: %w", err)
	}

	// Bind the Dead Letter Queue to the Dead Letter Exchange
	err = c.channel.QueueBind(
		dlqName, // queue name
		dlqName, // routing key (using queue name as routing key)
		dlxName, // exchange
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind DL queue to DL exchange: %w", err)
	}

	// Update the main queue to use the Dead Letter Exchange
	// First we need to delete the existing queue and recreate it with DLX parameters
	_, err = c.channel.QueueDelete(
		c.queueName, // name
		false,       // ifUnused
		false,       // ifEmpty
		false,       // noWait
	)
	if err != nil {
		return fmt.Errorf("failed to delete existing queue: %w", err)
	}

	// Re-declare the main queue with DLX arguments
	args := amqp.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": dlqName,
	}

	_, err = c.channel.QueueDeclare(
		c.queueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		args,        // arguments with DLX config
	)
	if err != nil {
		return fmt.Errorf("failed to redeclare queue with DLX: %w", err)
	}

	// Set quality of service
	err = c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Start consuming from the main queue
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

					// Add error information to headers
					headers := amqp.Table{}
					if d.Headers != nil {
						headers = d.Headers
					}
					headers["x-error-type"] = "unmarshal-error"
					headers["x-error-message"] = err.Error()
					headers["x-failed-at"] = time.Now().Format(time.RFC3339)

					// Publish directly to DLQ since we can't use Nack for format errors
					err = c.publishToDLQ(dlxName, dlqName, d.Body, headers)
					if err != nil {
						log.Printf("Failed to publish to DLQ: %v", err)
					}

					d.Ack(false) // Acknowledge to remove from original queue
					continue
				}

				err = processFunc(job)
				if err != nil {
					log.Printf("Error processing job %s: %s", job.JobID, err)

					// Track retry count
					headers := amqp.Table{}
					if d.Headers != nil {
						headers = d.Headers
					}

					var retryCount int32 = 1
					if val, exists := headers["x-retry-count"]; exists {
						if count, ok := val.(int32); ok {
							retryCount = count + 1
						}
					}

					headers["x-retry-count"] = retryCount
					headers["x-error-message"] = err.Error()
					headers["x-failed-at"] = time.Now().Format(time.RFC3339)
					headers["x-original-job-id"] = job.JobID

					// If we've tried too many times, send to DLQ
					maxRetries := int32(3) // Set your desired max retry count
					if retryCount > maxRetries {
						headers["x-max-retries-exceeded"] = true

						// Publish to DLQ
						err = c.publishToDLQ(dlxName, dlqName, d.Body, headers)
						if err != nil {
							log.Printf("Failed to publish to DLQ: %v", err)
						}

						d.Ack(false) // Acknowledge to remove from original queue
					} else {
						// Reject the message and requeue it for retry
						d.Nack(false, true)
					}

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

// Helper function to publish a message to the DLQ
func (c *RabbitMQClient) publishToDLQ(exchange, routingKey string, body []byte, headers amqp.Table) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.channel.PublishWithContext(
		ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Headers:      headers,
		},
	)
}

// New method to consume messages from the DLQ
func (c *RabbitMQClient) ConsumeDLQ(dlqHandler func(models.ResizeJob, amqp.Table) error) error {
	dlqName := c.queueName + ".dlq"

	msgs, err := c.channel.Consume(
		dlqName, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return fmt.Errorf("failed to register DLQ consumer: %w", err)
	}

	go func() {
		for d := range msgs {
			log.Printf("Processing message from DLQ: %v", d.MessageId)

			var job models.ResizeJob
			err := json.Unmarshal(d.Body, &job)
			if err != nil {
				log.Printf("Error unmarshaling job from DLQ: %v", err)
				d.Ack(false) // Remove malformed messages
				continue
			}

			// Call the handler
			err = dlqHandler(job, d.Headers)
			if err != nil {
				log.Printf("Error handling DLQ message: %v", err)
				// Requeue for later processing
				d.Nack(false, true)
			} else {
				// Successfully processed
				d.Ack(false)
			}
		}
	}()

	log.Printf("Started consuming from DLQ: %s", dlqName)
	return nil
}

// Method to retry a job from the DLQ
func (c *RabbitMQClient) RetryJob(job models.ResizeJob) error {
	return c.PublishJob(job)
}
