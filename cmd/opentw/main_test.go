package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// supervise обязан вернуть только после завершения ВСЕХ компонентов:
// именно тогда defer store.Close() в run() закрыть базу безопасно.
func TestSuperviseWaitsForAllComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slowDone := make(chan struct{})
	release := make(chan struct{})
	slow := func(context.Context) error {
		<-release // имитация финального цикла, дотягивающего работу после отмены
		close(slowDone)
		return nil
	}
	fast := func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	go func() { time.Sleep(60 * time.Millisecond); close(release) }()
	if err := supervise(ctx, cancel, []func(context.Context) error{fast, slow}); err != nil {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-slowDone:
	default:
		t.Fatal("supervise returned before the slow component finished")
	}
}

// Ошибка одного компонента отменяет второй, и supervise не возвращается,
// пока второй не завершится.
func TestSuperviseStopsSiblingOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	boom := func(context.Context) error { return errors.New("boom") }
	siblingDone := make(chan struct{})
	sibling := func(ctx context.Context) error {
		<-ctx.Done()
		close(siblingDone)
		return nil
	}
	err := supervise(ctx, cancel, []func(context.Context) error{sibling, boom})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-siblingDone:
	default:
		t.Fatal("sibling must be stopped before supervise returns")
	}
}
