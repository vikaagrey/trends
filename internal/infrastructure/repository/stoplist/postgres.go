package stoplist

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	stoplistrepo "github.com/vikagrej/trends/internal/repository/stoplist"
)

var ErrPostgresPoolRequired = errors.New("postgres pool is required")

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, ErrPostgresPoolRequired
	}
	_ = ctx
	return &PostgresRepository{pool: pool}, nil
}

func (repository *PostgresRepository) List(ctx context.Context) ([]string, error) {
	rows, err := repository.pool.Query(ctx, "SELECT word FROM stop_words ORDER BY word")
	if err != nil {
		return nil, fmt.Errorf("read stop_words: %w", err)
	}
	defer rows.Close()

	var words []string
	for rows.Next() {
		var word string
		if err := rows.Scan(&word); err != nil {
			return nil, fmt.Errorf("scan stop_words row: %w", err)
		}
		words = append(words, word)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stop_words rows: %w", err)
	}
	return words, nil
}

func (repository *PostgresRepository) Add(ctx context.Context, word string) error {
	commandTag, err := repository.pool.Exec(ctx,
		"INSERT INTO stop_words (word) VALUES ($1) ON CONFLICT DO NOTHING",
		word,
	)
	if err != nil {
		return fmt.Errorf("insert stop_words: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return stoplistrepo.ErrAlreadyExists
	}
	return nil
}

func (repository *PostgresRepository) Remove(ctx context.Context, word string) error {
	_, err := repository.pool.Exec(ctx, "DELETE FROM stop_words WHERE word = $1", word)
	if err != nil {
		return fmt.Errorf("delete stop_words: %w", err)
	}
	return nil
}
