package utils_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/distributed-systems/pkg/utils"
)

func TestRetry_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	err := utils.Retry(context.Background(), utils.DefaultRetryConfig(), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestRetry_SucceedsOnSecondAttempt(t *testing.T) {
	calls := 0
	cfg := utils.RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Multiplier: 2}
	err := utils.Retry(context.Background(), cfg, func() error {
		calls++
		if calls < 2 {
			return errors.New("temporary error")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestRetry_ExhaustsAttempts(t *testing.T) {
	calls := 0
	cfg := utils.RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Multiplier: 2}
	err := utils.Retry(context.Background(), cfg, func() error {
		calls++
		return errors.New("always fails")
	})
	if err == nil {
		t.Fatal("expected error after all attempts exhausted")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := utils.Retry(ctx, utils.DefaultRetryConfig(), func() error {
		return errors.New("should not run")
	})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestShortID(t *testing.T) {
	id := "550e8400-e29b-41d4"
	short := utils.ShortID(id)
	if len(short) != 8 {
		t.Errorf("expected 8 chars, got %d", len(short))
	}
	if short != "550e8400" {
		t.Errorf("expected '550e8400', got %q", short)
	}
}

func TestShortID_Short(t *testing.T) {
	id := "abc"
	short := utils.ShortID(id)
	if short != "abc" {
		t.Errorf("short ID should return as-is when < 8 chars")
	}
}
