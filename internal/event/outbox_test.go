package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type memoryStore struct {
	events      []OutboxEvent
	published   []uuid.UUID
	rescheduled []uuid.UUID
}

func (s *memoryStore) ClaimOutbox(context.Context, string, int, time.Duration) ([]OutboxEvent, error) {
	return append([]OutboxEvent(nil), s.events...), nil
}
func (s *memoryStore) MarkOutboxPublished(_ context.Context, id uuid.UUID, _ string) error {
	s.published = append(s.published, id)
	return nil
}
func (s *memoryStore) RescheduleOutbox(_ context.Context, id uuid.UUID, _ string, _ time.Time, _ string) error {
	s.rescheduled = append(s.rescheduled, id)
	return nil
}

type memoryBus struct {
	fail bool
	sent []uuid.UUID
}

func (b *memoryBus) Publish(_ context.Context, value OutboxEvent) error {
	if b.fail {
		return errors.New("模拟 NATS 不可用")
	}
	b.sent = append(b.sent, value.ID)
	return nil
}

func TestDispatcherMarksAckedEventPublished(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	store := &memoryStore{events: []OutboxEvent{{ID: id, Type: "task.ready"}}}
	bus := &memoryBus{}
	dispatcher := NewDispatcher(store, bus, "test-owner", time.Second, log.NewStdLogger(nil))

	count, err := dispatcher.DispatchOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, []uuid.UUID{id}, bus.sent)
	require.Equal(t, []uuid.UUID{id}, store.published)
	require.Empty(t, store.rescheduled)
}

func TestDispatcherReschedulesPublishFailure(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	store := &memoryStore{events: []OutboxEvent{{ID: id, Type: "task.ready"}}}
	bus := &memoryBus{fail: true}
	dispatcher := NewDispatcher(store, bus, "test-owner", time.Second, log.NewStdLogger(nil))

	count, err := dispatcher.DispatchOnce(context.Background())
	require.NoError(t, err, "单条发布失败会重排，不应终止整个批次")
	require.Equal(t, 1, count)
	require.Equal(t, []uuid.UUID{id}, store.rescheduled)
	require.Empty(t, store.published)
}
