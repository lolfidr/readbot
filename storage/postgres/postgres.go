package postgres

import (
	"context"
	"errors"
	"readbot/lib/e"
	"readbot/storage"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Проверка соответствия интерфейсу на этапе компиляции
var _ storage.Storage = (*Storage)(nil)

type Storage struct {
	db *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Storage, error) {
	const op = "storage.postgres.New"

	db, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	// Проверка соединения
	if err := db.Ping(ctx); err != nil {
		return nil, e.Wrap(op, err)
	}

	// Создание таблицы если не существует
	_, err = db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pages (
			id SERIAL PRIMARY KEY,
			url TEXT NOT NULL,
			user_name TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			UNIQUE(url, user_name)
		);
		
		CREATE INDEX IF NOT EXISTS idx_pages_user ON pages(user_name);
		CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);
	`)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) Close() {
	s.db.Close()
}

func (s *Storage) Save(ctx context.Context, p *storage.Page) error {
	const op = "storage.postgres.Save"

	q := `
		INSERT INTO pages (url, user_name)
		VALUES ($1, $2)
		ON CONFLICT (url, user_name) DO NOTHING
	`

	_, err := s.db.Exec(ctx, q, p.URL, p.UserName)
	if err != nil {
		return e.Wrap(op, err)
	}

	return nil
}

func (s *Storage) PickRandom(ctx context.Context, userName string) (*storage.Page, error) {
	const op = "storage.postgres.PickRandom"

	q := `
		SELECT url, user_name FROM pages
		WHERE user_name = $1
		ORDER BY RANDOM()
		LIMIT 1
	`

	var page storage.Page
	err := s.db.QueryRow(ctx, q, userName).Scan(&page.URL, &page.UserName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, storage.ErrNoSavedPages
		}
		return nil, e.Wrap(op, err)
	}

	return &page, nil
}

func (s *Storage) Remove(ctx context.Context, p *storage.Page) error {
	const op = "storage.postgres.Remove"

	q := `
		DELETE FROM pages
		WHERE url = $1 AND user_name = $2
	`

	res, err := s.db.Exec(ctx, q, p.URL, p.UserName)
	if err != nil {
		return e.Wrap(op, err)
	}

	if res.RowsAffected() == 0 {
		return storage.ErrURLNotFound
	}

	return nil
}

func (s *Storage) IsExists(ctx context.Context, p *storage.Page) (bool, error) {
	const op = "storage.postgres.IsExists"

	q := `
		SELECT EXISTS(
			SELECT 1 FROM pages
			WHERE url = $1 AND user_name = $2
		)
	`

	var exists bool
	err := s.db.QueryRow(ctx, q, p.URL, p.UserName).Scan(&exists)
	if err != nil {
		return false, e.Wrap(op, err)
	}

	return exists, nil
}

func (s *Storage) Init(ctx context.Context) error {
	const op = "storage.postgres.Init"

	_ = op

	// Таблица уже создается в New(), поэтому можно просто вернуть nil
	// или добавить дополнительную инициализацию если нужно
	return nil
}
