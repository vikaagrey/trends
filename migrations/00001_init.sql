-- +goose Up
CREATE TABLE stop_words (
    word TEXT PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE stop_words;
