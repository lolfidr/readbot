package main

import (
	"context"
	"flag"
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
	token := mustToken()
	dsn := mustPostgresDSN()

	// Инициализируем хранилище (таблица создается внутри New)
	pgStorage, err := postgres.New(context.Background(), dsn)
	if err != nil {
		log.Fatal("Failed to connect to PostgreSQL:", err)
	}
	defer pgStorage.Close()

	// Создаем клиент Telegram
	tgClient := tgClient.New(tgBotHost, token)

	// Создаем процессор событий
	eventsProcessor := telegram.New(tgClient, pgStorage)

	log.Print("service started")

	consumer := event_consumer.New(eventsProcessor, eventsProcessor, batchSize)

	if err := consumer.Start(); err != nil {
		log.Fatal("service is stopped", err)
	}
}

func mustToken() string {
	token := flag.String(
		"tg-bot-token",
		"",
		"token for access to telegram bot",
	)

	flag.Parse()

	if *token == "" {
		log.Fatal("token is not specified")
	}

	return *token
}

func mustPostgresDSN() string {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Fatal("PG_DSN environment variable is not set")
	}
	return dsn
}
