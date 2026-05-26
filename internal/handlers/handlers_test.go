package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vikagrej/trends/internal/handlers"
	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/stoplist"
	"github.com/vikagrej/trends/internal/stoplist/stoplisttest"
	"github.com/vikagrej/trends/internal/topn"
)

type fakeEngine struct {
	items []topn.Item
}

func (engine *fakeEngine) Top(limit int) topn.Snapshot {
	items := engine.items
	if items == nil {
		items = []topn.Item{}
	}
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return topn.Snapshot{Items: items, GeneratedAt: time.Now(), WindowSec: 300}
}

func newTestRouter(items []topn.Item, stopwords ...string) http.Handler {
	repo := stoplisttest.NewRepository(stopwords...)
	return newTestRouterWithRepo(items, repo)
}

func newTestRouterWithRepo(items []topn.Item, repo stoplist.Repository) http.Handler {
	engine := &fakeEngine{items: items}
	stoplistService := stoplist.NewService(repo)
	_ = stoplistService.Init(context.Background())
	return handlers.NewRouter(engine, stoplistService, nil, nil)
}

func doRequest(router http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	var request *http.Request
	if body != nil {
		request = httptest.NewRequest(method, path, bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
	} else {
		request = httptest.NewRequest(method, path, nil)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func TestGetTop_OK(t *testing.T) {
	items := []topn.Item{{Query: "golang", Count: 42}, {Query: "rust", Count: 17}}
	router := newTestRouter(items)

	response := doRequest(router, http.MethodGet, "/api/v1/top?n=2", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}

	var resp struct {
		Data      []topn.Item `json:"data"`
		WindowSec int         `json:"window_sec"`
	}
	if err := json.NewDecoder(response.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Errorf("expected 2 items, got %d", len(resp.Data))
	}
	if resp.WindowSec != 300 {
		t.Errorf("expected window_sec=300, got %d", resp.WindowSec)
	}
}

func TestGetTop_DefaultN(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/api/v1/top", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
}

func TestGetTop_BadN_NonNumeric(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/api/v1/top?n=abc", nil)
	if response.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "get top") {
		t.Fatalf("response leaks internal context: %s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "parameter 'n' must be a positive integer") {
		t.Fatalf("response has unexpected body: %s", response.Body.String())
	}
}

func TestGetTop_BadN_TooLarge(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/api/v1/top?n=9999", nil)
	if response.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "get top") {
		t.Fatalf("response leaks internal context: %s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "parameter 'n' must not exceed 1000") {
		t.Fatalf("response has unexpected body: %s", response.Body.String())
	}
}

func TestGetTop_EmptyList(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/api/v1/top?n=10", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	var resp struct {
		Data []topn.Item `json:"data"`
	}
	json.NewDecoder(response.Body).Decode(&resp)
	if resp.Data == nil {
		t.Error("data field should be [] not null")
	}
}

func TestListStoplist_OK(t *testing.T) {
	router := newTestRouter(nil, "spam", "ads")
	response := doRequest(router, http.MethodGet, "/api/v1/stoplist", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	var resp struct {
		Words []string `json:"words"`
	}
	json.NewDecoder(response.Body).Decode(&resp)
	if len(resp.Words) != 2 {
		t.Errorf("expected 2 words, got %d", len(resp.Words))
	}
}

func TestAddStopword_OK(t *testing.T) {
	router := newTestRouter(nil)
	body, _ := json.Marshal(map[string]string{"word": "spam"})
	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var resp map[string]string
	json.NewDecoder(response.Body).Decode(&resp)
	if resp["status"] != "added" {
		t.Errorf("expected status=added, got %q", resp["status"])
	}
}

func TestAddStopword_NormalizesWord(t *testing.T) {
	router := newTestRouter(nil)
	body, _ := json.Marshal(map[string]string{"word": " СПАМ  слово "})

	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var addResp map[string]string
	json.NewDecoder(response.Body).Decode(&addResp)
	if addResp["word"] != "спам слово" {
		t.Fatalf("expected normalized response word, got %q", addResp["word"])
	}

	listResponse := doRequest(router, http.MethodGet, "/api/v1/stoplist", nil)
	var resp struct {
		Words []string `json:"words"`
	}
	json.NewDecoder(listResponse.Body).Decode(&resp)
	if len(resp.Words) != 1 || resp.Words[0] != "спам слово" {
		t.Fatalf("expected normalized word, got %v", resp.Words)
	}
}

func TestAddStopword_AlreadyExists(t *testing.T) {
	router := newTestRouter(nil, "spam")
	body, _ := json.Marshal(map[string]string{"word": " SPAM "})

	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)

	if response.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d: %s", response.Code, response.Body.String())
	}
}

func TestAddStopword_EmptyWord(t *testing.T) {
	router := newTestRouter(nil)
	body, _ := json.Marshal(map[string]string{"word": ""})
	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)

	if response.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", response.Code)
	}
}

func TestAddStopword_MissingWord(t *testing.T) {
	router := newTestRouter(nil)
	body, _ := json.Marshal(map[string]string{})
	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)

	if response.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", response.Code)
	}
}

func TestAddStopword_InvalidJSON(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", []byte("{invalid}"))

	if response.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", response.Code)
	}
}

func TestAddStopword_UnsupportedContentType(t *testing.T) {
	router := newTestRouter(nil)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/stoplist", strings.NewReader(`{"word":"spam"}`))
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected 415, got %d: %s", response.Code, response.Body.String())
	}
}

func TestAddStopword_BodyTooLarge(t *testing.T) {
	router := newTestRouter(nil)
	body := []byte(`{"word":"` + strings.Repeat("x", 5000) + `"}`)
	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)

	if response.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", response.Code)
	}
}

func TestAddStopword_InternalErrorDoesNotLeakDetails(t *testing.T) {
	internalErr := errors.New("postgres password=secret sql details")
	repo := stoplisttest.NewRepository()
	repo.AddErr = internalErr
	router := newTestRouterWithRepo(nil, repo)
	body, _ := json.Marshal(map[string]string{"word": "spam"})

	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "sql details") {
		t.Fatalf("response leaks internal error: %s", response.Body.String())
	}
}

func TestRemoveStopword_OK(t *testing.T) {
	router := newTestRouter(nil, "spam")
	response := doRequest(router, http.MethodDelete, "/api/v1/stoplist/spam", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var resp map[string]string
	json.NewDecoder(response.Body).Decode(&resp)
	if resp["status"] != "removed" {
		t.Errorf("expected status=removed, got %q", resp["status"])
	}
}

func TestRemoveStopword_MissingWordIsOK(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodDelete, "/api/v1/stoplist/missing", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRemoveStopword_InvalidWord(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodDelete, "/api/v1/stoplist/%20%20", nil)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", response.Code, response.Body.String())
	}
}

func TestRemoveStopword_InternalErrorDoesNotLeakDetails(t *testing.T) {
	internalErr := errors.New("postgres password=secret sql details")
	repo := stoplisttest.NewRepository("spam")
	repo.RemoveErr = internalErr
	router := newTestRouterWithRepo(nil, repo)

	response := doRequest(router, http.MethodDelete, "/api/v1/stoplist/spam", nil)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "sql details") {
		t.Fatalf("response leaks internal error: %s", response.Body.String())
	}
}

func TestHealthCheck(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/healthz", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	var resp map[string]string
	json.NewDecoder(response.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", resp["status"])
	}
}

func TestCORSOptions(t *testing.T) {
	router := newTestRouter(nil)
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/top", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Errorf("OPTIONS should return 200, got %d", response.Code)
	}
	if response.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("CORS header missing")
	}
}

func TestCORSHeader_OnNormalRequest(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/api/v1/top", nil)
	if response.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS header should be * on normal GET")
	}
}

func TestCORSHeader_NotOnMutationRequest(t *testing.T) {
	router := newTestRouter(nil)
	body, _ := json.Marshal(map[string]string{"word": "spam"})
	response := doRequest(router, http.MethodPost, "/api/v1/stoplist", body)
	if response.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("CORS header should not be set on POST")
	}
}

func TestMetricsEndpoint_NotRegisteredWithoutRegistry(t *testing.T) {
	router := newTestRouter(nil)
	response := doRequest(router, http.MethodGet, "/metrics", nil)
	if response.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", response.Code)
	}
}

func TestHTTPMetricsUsesRoutePattern(t *testing.T) {
	registry := metrics.NewRegistry()
	engine := &fakeEngine{}
	repo := stoplisttest.NewRepository("spam")
	stoplistService := stoplist.NewService(repo)
	_ = stoplistService.Init(context.Background())
	router := handlers.NewRouter(engine, stoplistService, registry, nil)

	deleteResponse := doRequest(router, http.MethodDelete, "/api/v1/stoplist/spam", nil)
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", deleteResponse.Code, deleteResponse.Body.String())
	}

	metricsResponse := doRequest(router, http.MethodGet, "/metrics", nil)
	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", metricsResponse.Code)
	}

	body := metricsResponse.Body.String()
	if !strings.Contains(body, `path="/api/v1/stoplist/{word}"`) {
		t.Fatalf("metrics should use route pattern, got body:\n%s", body)
	}
	if strings.Contains(body, "/api/v1/stoplist/spam") {
		t.Fatalf("metrics should not include raw request path, got body:\n%s", body)
	}
	if strings.Contains(body, `path="/metrics"`) {
		t.Fatalf("metrics endpoint should not instrument itself, got body:\n%s", body)
	}
}
