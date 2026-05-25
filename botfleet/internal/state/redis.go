package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"Ranabhum/bot-fleet/internal/bot"
	"github.com/redis/go-redis/v9"
)

const (
	runKeyPrefix = "run:"
	runTTL       = 2 * time.Hour
)

// Store manages run state in Redis.
type Store struct {
	rdb *redis.Client
}

func New(addr string) *Store {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, // e.g. "redis:6379"
	})
	return &Store{rdb: rdb}
}

func (s *Store) SetRun(ctx context.Context, run bot.RunState) error {
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s%s", runKeyPrefix, run.RunID)
	return s.rdb.Set(ctx, key, data, runTTL).Err()
}

func (s *Store) GetRun(ctx context.Context, runID string) (*bot.RunState, error) {
	key := fmt.Sprintf("%s%s", runKeyPrefix, runID)
	data, err := s.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	var run bot.RunState
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

func (s *Store) UpdateStatus(ctx context.Context, runID, status string) error {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	run.Status = status
	if status == "DONE" || status == "FAILED" {
		run.EndedAt = time.Now().UnixNano()
	}
	return s.SetRun(ctx, *run)
}

func (s *Store) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}
