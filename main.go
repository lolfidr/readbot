package main

import (
	"context"
	"log"
	"os"
	tgClient "readbot/clients/telegram"
	event_consumer "readbot/consumer/event-consumer"
	"readbot/events/telegram"
	"readbot/storage/postgres"
)

const (
	tgBotHost = "api.telegram.org"
	batchSize = 100
)

func main() {
	// Получаем конфигурацию из переменных окружения
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Fatal("PG_DSN environment variable is not set")
	}

	botToken := os.Getenv("TG_BOT_TOKEN")
	if botToken == "" {
		log.Fatal("TG_BOT_TOKEN environment variable is not set")
	}

	// Инициализация хранилища
	pgStorage, err := postgres.New(context.Background(), dsn)
	if err != nil {
		log.Fatal("Failed to connect to PostgreSQL:", err)
	}
	defer pgStorage.Close()

	// Инициализация бота
	eventsProcessor := telegram.New(
		tgClient.New(tgBotHost, botToken),
		pgStorage,
	)

	log.Print("service started")

	// Запуск потребителя событий
	consumer := event_consumer.New(eventsProcessor, eventsProcessor, batchSize)
	if err := consumer.Start(); err != nil {
		log.Fatal("service is stopped", err)
	}
}
