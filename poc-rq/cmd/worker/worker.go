package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/vatsal3003/poc-rq/internal/config"
	"github.com/vatsal3003/poc-rq/internal/rabbitmq"
	"github.com/vatsal3003/poc-rq/internal/resize"
)

func main() {
	cfg := config.NewConfig()

	processor := resize.NewImageProcessor(cfg.ResizeConfigs)

	// Connect to RabbitMQ
	mqClient, err := rabbitmq.NewRabbitMQClient(cfg.RabbitMQURL, cfg.QueueName)
	if err != nil {
		log.Println("ERROR failed to connect to rabbitmq:", err.Error())
		return
	}
	defer mqClient.Close()

	log.Println("INFO starting worker")

	// Graceful shutdown
	stopChan := make(chan struct{})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		close(stopChan)
		mqClient.Close()
	}()

	err = mqClient.ConsumeJobs(processor.ProcessJob, stopChan)
	if err != nil {
		log.Println("ERROR failed to start consuming jobs:", err.Error())
		return
	}
}
