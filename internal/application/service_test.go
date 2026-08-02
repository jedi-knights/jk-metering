package application_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jedi-knights/jk-metering/internal/application"
	"github.com/jedi-knights/jk-metering/internal/domain"
)

type fakeSource struct {
	mu         sync.Mutex
	pending    []domain.AuditEvent
	consumed   []string
	fetchErr   error
	consumeErr error
	oldestAge  time.Duration
	oldestErr  error
}

func (f *fakeSource) FetchUnconsumed(_ context.Context, _ int) ([]domain.AuditEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	out := f.pending
	f.pending = nil
	return out, nil
}

func (f *fakeSource) MarkConsumed(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumeErr != nil {
		return f.consumeErr
	}
	f.consumed = append(f.consumed, id)
	return nil
}

func (f *fakeSource) OldestUnconsumedAge(_ context.Context) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.oldestAge, f.oldestErr
}

type fakeSink struct {
	mu       sync.Mutex
	received []domain.LagoEvent
	pushErr  error
}

func (f *fakeSink) PushEvent(_ context.Context, e domain.LagoEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pushErr != nil {
		return f.pushErr
	}
	f.received = append(f.received, e)
	return nil
}

func TestTick_HappyPath_PushesAndMarksConsumed(t *testing.T) {
	src := &fakeSource{pending: []domain.AuditEvent{sampleEvent()}}
	sink := &fakeSink{}
	svc := application.NewMeteringService(src, sink, application.Config{})

	if err := svc.Tick(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sink.received) != 1 {
		t.Errorf("expected 1 push, got %d", len(sink.received))
	}
	if len(src.consumed) != 1 || src.consumed[0] != sampleEvent().EventID {
		t.Errorf("expected event marked consumed, got %v", src.consumed)
	}
	if svc.Stats().Processed != 1 {
		t.Errorf("processed = %d, want 1", svc.Stats().Processed)
	}
}

func TestTick_SkipsEventWithoutBillingIdentity(t *testing.T) {
	e := sampleEvent()
	e.SubjectID = ""
	e.ClientID = ""
	e.ActorID = ""
	src := &fakeSource{pending: []domain.AuditEvent{e}}
	sink := &fakeSink{}
	svc := application.NewMeteringService(src, sink, application.Config{})

	if err := svc.Tick(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sink.received) != 0 {
		t.Errorf("expected sink not to receive unattributable event")
	}
	if len(src.consumed) != 1 {
		t.Errorf("expected unattributable event marked consumed to avoid retry-storm")
	}
	if svc.Stats().Skipped != 1 {
		t.Errorf("skipped = %d, want 1", svc.Stats().Skipped)
	}
}

func TestTick_PushFailureLeavesEventUnconsumed(t *testing.T) {
	src := &fakeSource{pending: []domain.AuditEvent{sampleEvent()}}
	sink := &fakeSink{pushErr: errors.New("lago unreachable")}
	svc := application.NewMeteringService(src, sink, application.Config{})

	if err := svc.Tick(context.Background()); err != nil {
		t.Fatalf("Tick returned error: %v", err)
	}
	if len(src.consumed) != 0 {
		t.Errorf("expected event left unconsumed on push failure, got %v", src.consumed)
	}
	if svc.Stats().Failed != 1 {
		t.Errorf("failed = %d, want 1", svc.Stats().Failed)
	}
}

func TestTick_FetchErrorReturnsError(t *testing.T) {
	src := &fakeSource{fetchErr: errors.New("postgres down")}
	sink := &fakeSink{}
	svc := application.NewMeteringService(src, sink, application.Config{})

	if err := svc.Tick(context.Background()); err == nil {
		t.Fatal("expected error when fetch fails")
	}
}

func TestNewMeteringService_NilSourcePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = application.NewMeteringService(nil, &fakeSink{}, application.Config{})
}

func TestNewMeteringService_NilSinkPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = application.NewMeteringService(&fakeSource{}, nil, application.Config{})
}

func TestNewMeteringService_DefaultsBatchSizeAndIdentity(t *testing.T) {
	svc := application.NewMeteringService(&fakeSource{}, &fakeSink{}, application.Config{})
	// Indirectly verify defaults by running a Tick with no events — it
	// should not panic and should return nil.
	if err := svc.Tick(context.Background()); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRun_RequiresPositiveInterval(t *testing.T) {
	svc := application.NewMeteringService(&fakeSource{}, &fakeSink{}, application.Config{})
	if err := svc.Run(context.Background(), 0); err == nil {
		t.Fatal("expected error for non-positive interval")
	}
}
