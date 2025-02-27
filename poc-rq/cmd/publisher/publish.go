package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/vatsal3003/poc-rq/internal/config"
	"github.com/vatsal3003/poc-rq/internal/rabbitmq"
	"github.com/vatsal3003/poc-rq/pkg/models"
)

func main() {
	if len(os.Args) < 2 {
		log.Printf("Usage: %s [imagePath]\n", os.Args[0])
		return
	}

	imagePath := os.Args[1]
	cfg := config.NewConfig()

	// Connect to RabbitMQ
	mqClient, err := rabbitmq.NewRabbitMQClient(cfg.RabbitMQURL, cfg.QueueName)
	if err != nil {
		log.Println("ERROR failed to connect to rabbitmq:", err.Error())
		return
	}
	defer mqClient.Close()

	outputDir := filepath.Join("resized", time.Now().Format("20060102150405"))

	job := models.NewResizeJob(imagePath, outputDir)

	err = mqClient.PublishJob(job)
	if err != nil {
		log.Println("ERROR failed to publish job:", err.Error())
		return
	}

	fmt.Printf("Published resize job %s for image %s\n", job.JobID, imagePath)
}
