package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.uber.org/zap"

	"github.com/vikagrej/trends/internal/domain/model"
	appLogger "github.com/vikagrej/trends/internal/logger"
	stoplistuc "github.com/vikagrej/trends/internal/usecase/stoplist"
	trendsuc "github.com/vikagrej/trends/internal/usecase/trends"
)

const maxStopwordBodyBytes = 4 << 10

type Handler struct {
	useCase *trendsuc.UseCase
	logger  *zap.Logger
}

func New(useCase *trendsuc.UseCase, logger *zap.Logger) *Handler {
	return &Handler{useCase: useCase, logger: appLogger.Safe(logger)}
}

func (handler *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/top", handler.getTop)
	mux.HandleFunc("GET /api/v1/stoplist", handler.listStoplist)
	mux.HandleFunc("POST /api/v1/stoplist", handler.addStopword)
	mux.HandleFunc("DELETE /api/v1/stoplist/{word}", handler.removeStopword)
	mux.HandleFunc("GET /healthz", handler.healthCheck)
}

func (handler *Handler) getTop(w http.ResponseWriter, r *http.Request) {
	result, err := handler.useCase.GetTop(r.URL.Query().Get("n"))
	if err != nil {
		handler.logger.Info("Invalid top request", zap.Error(err))
		if respondTopClientError(w, err) {
			return
		}
		respondError(w, http.StatusBadRequest, "invalid top limit")
		return
	}

	respondJSON(w, http.StatusOK, struct {
		Data      []model.TopItem `json:"data"`
		Timestamp int64           `json:"timestamp"`
		WindowSec int             `json:"window_sec"`
	}{
		Data:      result.Data,
		Timestamp: result.Timestamp,
		WindowSec: result.WindowSec,
	})
}

func (handler *Handler) listStoplist(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, struct {
		Words []string `json:"words"`
	}{Words: handler.useCase.ListStoplist()})
}

func (handler *Handler) addStopword(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxStopwordBodyBytes)
	var requestBody struct {
		Word string `json:"word"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if requestBody.Word == "" {
		respondError(w, http.StatusBadRequest, "word is required")
		return
	}

	normalizedWord, err := handler.useCase.AddStopword(r.Context(), requestBody.Word)
	if err != nil {
		if respondStopwordClientError(w, err) {
			return
		}
		handler.logger.Error("Failed to add stopword", zap.String("word", requestBody.Word), zap.Error(err))
		respondError(w, http.StatusInternalServerError, "failed to add stopword")
		return
	}

	respondJSON(w, http.StatusOK, struct {
		Word   string `json:"word"`
		Status string `json:"status"`
	}{Word: normalizedWord, Status: "added"})
}

func (handler *Handler) removeStopword(w http.ResponseWriter, r *http.Request) {
	word := r.PathValue("word")
	if word == "" {
		respondError(w, http.StatusBadRequest, "word is required")
		return
	}

	if err := handler.useCase.RemoveStopword(r.Context(), word); err != nil {
		if respondStopwordClientError(w, err) {
			return
		}
		handler.logger.Error("Failed to remove stopword", zap.String("word", word), zap.Error(err))
		respondError(w, http.StatusInternalServerError, "failed to remove stopword")
		return
	}

	respondJSON(w, http.StatusOK, struct {
		Word   string `json:"word"`
		Status string `json:"status"`
	}{Word: word, Status: "removed"})
}

func (handler *Handler) healthCheck(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func respondTopClientError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, trendsuc.ErrTopLimitNotPositive):
		respondError(w, http.StatusBadRequest, trendsuc.ErrTopLimitNotPositive.Error())
	case errors.Is(err, trendsuc.ErrTopLimitTooLarge):
		respondError(w, http.StatusBadRequest, trendsuc.ErrTopLimitTooLarge.Error())
	default:
		return false
	}
	return true
}

func respondStopwordClientError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, stoplistuc.ErrInvalidWord):
		respondError(w, http.StatusBadRequest, "invalid stopword")
	case errors.Is(err, stoplistuc.ErrAlreadyExists):
		respondError(w, http.StatusConflict, "stopword already exists")
	default:
		return false
	}
	return true
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	_ = writeJSON(w, status, data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	_ = writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, data any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(data)
}
