package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/vikagrej/trends/internal/consumer"
)

func main() {
	brokers := flag.String("brokers", "localhost:29092", "kafka brokers (csv)")
	topic := flag.String("topic", "search.events", "kafka topic")
	n := flag.Int("n", 1000, "number of events to send")
	query := flag.String("query", "", "если задан, все события будут с этим query")
	sources := flag.Int("sources", 0, "число уникальных source для фиксированного query; 0 = n")
	flood := flag.Bool("flood", false, "демонстрация анти-накрутки: один bot-источник шлёт один и тот же запрос 1000 раз")
	flag.Parse()

	w := &kafka.Writer{
		Addr:     kafka.TCP(*brokers),
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	var events []consumer.SearchEvent

	if *query != "" {
		uniqueSources := *sources
		if uniqueSources <= 0 || uniqueSources > *n {
			uniqueSources = *n
		}
		for i := 0; i < *n; i++ {
			events = append(events, consumer.SearchEvent{
				Query:  *query,
				Source: fmt.Sprintf("e2e-user-%d", i%uniqueSources),
				TsMs:   now.UnixMilli(),
			})
		}
	} else if *flood {
		for i := 0; i < 1000; i++ {
			events = append(events, consumer.SearchEvent{
				Query:  "накрутка",
				Source: "bot-account",
				TsMs:   now.Add(-time.Duration(rand.Intn(300)) * time.Second).UnixMilli(),
			})
		}
		queries := []string{"слайм", "sixseven", "лабубу", "стикеры"}
		for i := 0; i < 200; i++ {
			events = append(events, consumer.SearchEvent{
				Query:  queries[rand.Intn(len(queries))],
				Source: fmt.Sprintf("user-%d", rand.Intn(5_000)),
				TsMs:   now.Add(-time.Duration(rand.Intn(300)) * time.Second).UnixMilli(),
			})
		}
	} else {
		queries := []string{
			"слайм", "sixseven", "лабубу", "сквиш", "поп ит",
			"стикеры", "брелок", "плюшевая игрушка", "кукла",
			"конструктор", "тамагочи", "шоппер", "худи", "кроксы",
			"бисер", "набор для творчества", "антистресс", "пенал",
		}
		for i := 0; i < *n; i++ {
			events = append(events, consumer.SearchEvent{
				Query:  queries[rand.Intn(len(queries))],
				Source: fmt.Sprintf("user-%d", rand.Intn(10_000)),
				TsMs:   now.Add(-time.Duration(rand.Intn(300)) * time.Second).UnixMilli(),
			})
		}
	}

	msgs := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		eventBytes, _ := json.Marshal(event)
		msgs = append(msgs, kafka.Message{Value: eventBytes})
	}

	if err := w.WriteMessages(ctx, msgs...); err != nil {
		log.Fatalf("write failed: %v", err)
	}

	fmt.Fprintf(os.Stdout, "sent %d events to %s\n", len(msgs), *topic)
	if *flood {
		fmt.Println(`Подождите ~2 сек и выполните: curl "http://localhost:8080/api/v1/top?n=20" | jq '.data[] | select(.query=="накрутка")'`)
		fmt.Println("'накрутка' должна появиться с count=1 (один уникальный источник).")
	}
}
