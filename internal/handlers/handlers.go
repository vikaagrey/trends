package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.uber.org/zap"

	appLogger "github.com/vikagrej/trends/internal/logger"
	"github.com/vikagrej/trends/internal/service"
	"github.com/vikagrej/trends/internal/stoplist"
)

const maxStopwordBodyBytes = 4 << 10

type Handler struct {
	service *service.TrendsService
	logger  *zap.Logger
}

func NewHandler(trendsService *service.TrendsService, logger *zap.Logger) *Handler {
	return &Handler{service: trendsService, logger: appLogger.Safe(logger)}
}

func (handler *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/top", handler.GetTop)
	mux.HandleFunc("GET /api/v1/stoplist", handler.ListStoplist)
	mux.HandleFunc("POST /api/v1/stoplist", handler.AddStopword)
	mux.HandleFunc("DELETE /api/v1/stoplist/{word}", handler.RemoveStopword)
	mux.HandleFunc("GET /healthz", handler.HealthCheck)
}

func (handler *Handler) GetTop(w http.ResponseWriter, r *http.Request) {
	result, err := handler.service.GetTop(r.URL.Query().Get("n"))
	if err != nil {
		handler.logger.Info("Invalid top request", zap.Error(err))
		if respondTopClientError(w, err) {
			return
		}
		respondError(w, http.StatusBadRequest, "invalid top limit")
		return
	}

	respondJSON(w, http.StatusOK, TopResponse{
		Data:      result.Data,
		Timestamp: result.Timestamp,
		WindowSec: result.WindowSec,
	})
}

func (handler *Handler) ListStoplist(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, StoplistResponse{Words: handler.service.ListStoplist()})
}

func (handler *Handler) AddStopword(w http.ResponseWriter, r *http.Request) {
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

	normalizedWord, err := handler.service.AddStopword(r.Context(), requestBody.Word)
	if err != nil {
		if respondStopwordClientError(w, err) {
			return
		}
		handler.logger.Error("Failed to add stopword", zap.String("word", requestBody.Word), zap.Error(err))
		respondError(w, http.StatusInternalServerError, "failed to add stopword")
		return
	}

	respondJSON(w, http.StatusOK, StopwordResponse{Word: normalizedWord, Status: "added"})
}

func (handler *Handler) RemoveStopword(w http.ResponseWriter, r *http.Request) {
	word := r.PathValue("word")
	if word == "" {
		respondError(w, http.StatusBadRequest, "word is required")
		return
	}

	if err := handler.service.RemoveStopword(r.Context(), word); err != nil {
		if respondStopwordClientError(w, err) {
			return
		}
		handler.logger.Error("Failed to remove stopword", zap.String("word", word), zap.Error(err))
		respondError(w, http.StatusInternalServerError, "failed to remove stopword")
		return
	}

	respondJSON(w, http.StatusOK, StopwordResponse{Word: word, Status: "removed"})
}

func (handler *Handler) HealthCheck(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func respondTopClientError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, service.ErrTopLimitNotPositive):
		respondError(w, http.StatusBadRequest, service.ErrTopLimitNotPositive.Error())
	case errors.Is(err, service.ErrTopLimitTooLarge):
		respondError(w, http.StatusBadRequest, service.ErrTopLimitTooLarge.Error())
	default:
		return false
	}
	return true
}

func respondStopwordClientError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, stoplist.ErrInvalidWord):
		respondError(w, http.StatusBadRequest, "invalid stopword")
	case errors.Is(err, stoplist.ErrAlreadyExists):
		respondError(w, http.StatusConflict, "stopword already exists")
	default:
		return false
	}
	return true
}
