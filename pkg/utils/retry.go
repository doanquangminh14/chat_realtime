// Package utils provides shared utility functions used across the project.
package utils

import (
	"context"
	"fmt"
	"math"
	"time"
)

// RetryConfig holds configuration for the retry mechanism.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// DefaultRetryConfig returns sensible retry defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   500 * time.Millisecond,
		MaxDelay:    10 * time.Second,
		Multiplier:  2.0,
	}
}

// Retry executes fn with exponential backoff until it succeeds or
// the max number of attempts is reached. The context is checked
// before each attempt to support cancellation.
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled after %d attempts: %w", attempt, ctx.Err())
		default:
		}

		if lastErr = fn(); lastErr == nil {
			return nil
		}

		if attempt == cfg.MaxAttempts-1 {
			break
		}

		delay := time.Duration(float64(cfg.BaseDelay) * math.Pow(cfg.Multiplier, float64(attempt)))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled during backoff: %w", ctx.Err())
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("all %d attempts failed, last error: %w", cfg.MaxAttempts, lastErr)
}
