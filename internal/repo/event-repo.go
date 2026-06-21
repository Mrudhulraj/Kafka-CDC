package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Event struct {
	EventID   string    `db:"event_id"`
	EventName string    `db:"event_type"`
	TimeStamp time.Time `db:"timestamp"`
}

func NewEvent() *Event {
	id := GenerateRandomStirng(15)
	return &Event{
		EventID:   id,
		EventName: "test_event",
		TimeStamp: time.Now(),
	}
}

type EventRepo struct {
	repo      *sqlx.DB
	tableName string
}

func NewEventRepo(db *sqlx.DB) *EventRepo {
	return &EventRepo{
		repo:      db,
		tableName: "events",
	}
}

func (r *EventRepo) Insert(ctx context.Context, tx *sqlx.Tx, e *Event) (string, error) {
	_, err := tx.NamedExecContext(ctx, fmt.Sprintf("INSERT INTO %s (event_name, event_id, created) VALUES(:event_name, :event_id, :created)", r.tableName), e)
	if err != nil {
		fmt.Printf("err on insert = %v\n", err)
		return "", err
	}
	return e.EventID, nil
}

func (r *EventRepo) Get(ctx context.Context, tx *sqlx.Tx, eventID string) *Event {
	e := &Event{}
	q := fmt.Sprintf("SELECT event_id from %s WHERE event_id = $1", r.tableName)
	err := tx.GetContext(ctx, e, q, eventID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
	}
	return e
}

func TxClosure[T any](ctx context.Context, r *EventRepo, fn func(ctx context.Context, tx *sqlx.Tx) (T, error)) (T, error) {
	tx, err := r.repo.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		panic("unable to start TX")
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}

		if err != nil {
			tx.Rollback()
			return
		}

		err = tx.Commit()
		if err != nil {
			fmt.Printf("err on commit = %v\n", err)
		}
	}()

	res, err := fn(ctx, tx)
	return res, err
}
