-- +goose Up
CREATE TABLE IF NOT EXISTS stop_words (
    word TEXT PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS stop_words;
