package handlers

import (
	"net/http"

	"github.com/vikagrej/trends/internal/httpx"
	"github.com/vikagrej/trends/internal/topn"
)

type TopResponse struct {
	Data      []topn.Item `json:"data"`
	Timestamp int64       `json:"timestamp"`
	WindowSec int         `json:"window_sec"`
}

type StoplistResponse struct {
	Words []string `json:"words"`
}

type StopwordResponse struct {
	Word   string `json:"word"`
	Status string `json:"status"`
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	_ = httpx.JSON(w, status, data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	_ = httpx.Error(w, status, message)
}
