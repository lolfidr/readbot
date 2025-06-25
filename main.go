package main

import (
	"context"
	"log"
	"os"
	tgClient "readbot/clients/telegram"
	event_consumer "readbot/consumer/event-consumer"
	"readbot/events/telegram"
	"readbot/storage/postgres"

	"github.com/joho/godotenv"
)

const (
	tgBotHost = "api.telegram.org"
	batchSize = 100
)

func main() {
	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}

	// Получение конфигурации
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Fatal("PG_DSN must be set in .env file")
	}

	botToken := os.Getenv("TG_BOT_TOKEN")
	if botToken == "" {
		log.Fatal("TG_BOT_TOKEN must be set in .env file")
	}

	// Инициализация хранилища
	pgStorage, err := postgres.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgStorage.Close()

	log.Println("Successfully connected to PostgreSQL!")

	// Инициализация Telegram клиента
	tgClient := tgClient.New(tgBotHost, botToken)

	// Создание процессора событий
	eventsProcessor := telegram.New(tgClient, pgStorage)

	log.Print("Service started")

	// Запуск потребителя событий
	consumer := event_consumer.New(eventsProcessor, eventsProcessor, batchSize)
	if err := consumer.Start(); err != nil {
		log.Fatal("Service stopped:", err)
	}
}
