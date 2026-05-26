package migrations

import "embed"

// Files contains embedded goose SQL migrations.
//
//go:embed *.sql
var Files embed.FS
