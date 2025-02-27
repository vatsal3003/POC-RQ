package config

import (
	"os"

	"github.com/vatsal3003/poc-rq/pkg/models"
)

type Config struct {
	RabbitMQURL   string
	QueueName     string
	ResizeConfigs []models.ResizeConfig
}

func NewConfig() *Config {
	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}

	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		queueName = "image_resize_jobs"
	}

	resizeConfigs := []models.ResizeConfig{
		{Width: 128, Height: 128, Suffix: "small"},
		{Width: 256, Height: 256, Suffix: "medium"},
		{Width: 512, Height: 512, Suffix: "large"},
		{Width: 1920, Height: 1080, Suffix: "hd"},
	}

	return &Config{
		RabbitMQURL:   rabbitMQURL,
		QueueName:     queueName,
		ResizeConfigs: resizeConfigs,
	}
}
