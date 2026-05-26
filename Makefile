.PHONY: up down test bench load-test producer clean

up:
	docker compose -f deploy/docker-compose.yml build trends
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

test:
	go test -race -count=1 -short ./internal/...

bench:
	go test -bench=. -benchmem -benchtime=3s ./internal/topn/

load-test:
	go test -race -count=1 -run TestLoadStress ./internal/handlers/

producer:
	go run ./scripts/producer -brokers localhost:29092 -topic search.events -n 5000

clean:
	rm -rf bin
