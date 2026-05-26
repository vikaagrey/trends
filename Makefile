.PHONY: up down test bench load-test producer clean

up:
	docker compose build trends
	docker compose up -d

down:
	docker compose down

test:
	go test -race -count=1 -short ./internal/...

bench:
	go test -bench=. -benchmem -benchtime=3s ./internal/infrastructure/topn/

load-test:
	go test -race -count=1 -run TestLoadStress ./internal/transport/http/

producer:
	go run ./scripts/producer -brokers localhost:29092 -topic search.events -n 5000

clean:
	rm -rf bin
