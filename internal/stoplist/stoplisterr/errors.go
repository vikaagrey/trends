package stoplisterr

import "errors"

var (
	ErrInvalidWord   = errors.New("invalid stopword")
	ErrAlreadyExists = errors.New("stopword already exists")
)
