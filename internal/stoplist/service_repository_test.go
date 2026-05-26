package stoplist_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vikagrej/trends/internal/stoplist"
	"github.com/vikagrej/trends/internal/stoplist/stoplisttest"
)

var errDB = errors.New("db error")

func TestService_Init_RepoError(t *testing.T) {
	repo := stoplisttest.NewRepository()
	repo.ListErr = errDB

	svc := stoplist.NewService(repo)
	err := svc.Init(context.Background())

	if !errors.Is(err, errDB) {
		t.Fatalf("Init() error = %v, want %v", err, errDB)
	}
}

func TestService_Add_RepoError(t *testing.T) {
	repo := stoplisttest.NewRepository()
	repo.AddErr = errDB

	svc := stoplist.NewService(repo)
	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}

	_, err := svc.Add(context.Background(), "spam")

	if !errors.Is(err, errDB) {
		t.Fatalf("Add() error = %v, want %v", err, errDB)
	}
	if got := svc.Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot()=%v, want empty cache after failed write", got)
	}
}

func TestService_Remove_RepoError(t *testing.T) {
	repo := stoplisttest.NewRepository("spam")
	repo.RemoveErr = errDB

	svc := stoplist.NewService(repo)
	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}

	err := svc.Remove(context.Background(), "spam")

	if !errors.Is(err, errDB) {
		t.Fatalf("Remove() error = %v, want %v", err, errDB)
	}
	if !stoplisttest.ContainsWord(svc.Snapshot(), "spam") {
		t.Fatal(`Snapshot() does not contain "spam"; cache must stay unchanged after failed write`)
	}
}

func TestService_Add_UpdatesCache_OnSuccess(t *testing.T) {
	repo := stoplisttest.NewRepository()
	svc := stoplist.NewService(repo)

	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}
	if _, err := svc.Add(context.Background(), "new"); err != nil {
		t.Fatalf("Add() error = %v, want nil", err)
	}

	if !stoplisttest.ContainsWord(svc.Snapshot(), "new") {
		t.Fatal(`Snapshot() does not contain "new" after successful Add`)
	}
}

func TestService_Remove_UpdatesCache_OnSuccess(t *testing.T) {
	repo := stoplisttest.NewRepository("ads")
	svc := stoplist.NewService(repo)

	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}
	if err := svc.Remove(context.Background(), "ads"); err != nil {
		t.Fatalf("Remove() error = %v, want nil", err)
	}

	if stoplisttest.ContainsWord(svc.Snapshot(), "ads") {
		t.Fatal(`Snapshot() still contains "ads" after successful Remove`)
	}
}
