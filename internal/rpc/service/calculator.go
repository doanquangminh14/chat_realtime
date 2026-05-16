package service

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/distributed-systems/internal/logger"
	"go.uber.org/zap"
)

// CalculationEvent represents a calculation event to be published
type CalculationEvent struct {
	RequestID         string    `json:"request_id"`
	Operation         string    `json:"operation"`
	Operands          []float64 `json:"operands"`
	Result            float64   `json:"result"`
	Timestamp         time.Time `json:"timestamp"`
	ComputationTimeNs int64     `json:"computation_time_ns"`
	Status            string    `json:"status"`
	ErrorMessage      string    `json:"error_message,omitempty"`
}

// EventPublisher defines the interface for publishing calculation events
type EventPublisher interface {
	Publish(ctx context.Context, event CalculationEvent) error
}

// CalculatorService implements the business logic for calculator operations
type CalculatorService struct {
	log       *logger.Logger
	publisher EventPublisher
}

// NewCalculatorService creates a new CalculatorService
func NewCalculatorService(log *logger.Logger, publisher EventPublisher) *CalculatorService {
	return &CalculatorService{
		log:       log.WithComponent("calculator-service"),
		publisher: publisher,
	}
}

// Add performs addition and publishes an event
func (s *CalculatorService) Add(ctx context.Context, requestID string, a, b float64) (float64, error) {
	start := time.Now()
	result := a + b
	elapsed := time.Since(start)

	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "add",
		Operands:          []float64{a, b},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	s.log.Debug("add computed", zap.Float64("a", a), zap.Float64("b", b), zap.Float64("result", result))
	return result, nil
}

// Subtract performs subtraction and publishes an event
func (s *CalculatorService) Subtract(ctx context.Context, requestID string, a, b float64) (float64, error) {
	start := time.Now()
	result := a - b
	elapsed := time.Since(start)

	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "subtract",
		Operands:          []float64{a, b},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	return result, nil
}

// Multiply performs multiplication and publishes an event
func (s *CalculatorService) Multiply(ctx context.Context, requestID string, a, b float64) (float64, error) {
	start := time.Now()
	result := a * b
	elapsed := time.Since(start)

	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "multiply",
		Operands:          []float64{a, b},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	return result, nil
}

// Divide performs division and returns an error for division by zero
func (s *CalculatorService) Divide(ctx context.Context, requestID string, a, b float64) (float64, error) {
	start := time.Now()

	if b == 0 {
		err := fmt.Errorf("division by zero is undefined")
		s.publishEvent(ctx, CalculationEvent{
			RequestID:         requestID,
			Operation:         "divide",
			Operands:          []float64{a, b},
			Timestamp:         time.Now(),
			ComputationTimeNs: time.Since(start).Nanoseconds(),
			Status:            "error",
			ErrorMessage:      err.Error(),
		})
		return 0, err
	}

	result := a / b
	elapsed := time.Since(start)

	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "divide",
		Operands:          []float64{a, b},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	return result, nil
}

// Power computes a^b and publishes an event
func (s *CalculatorService) Power(ctx context.Context, requestID string, base, exp float64) (float64, error) {
	start := time.Now()
	result := math.Pow(base, exp)
	elapsed := time.Since(start)

	if math.IsInf(result, 0) || math.IsNaN(result) {
		err := fmt.Errorf("power result is not a finite number: base=%.4f, exp=%.4f", base, exp)
		s.publishEvent(ctx, CalculationEvent{
			RequestID:         requestID,
			Operation:         "power",
			Operands:          []float64{base, exp},
			Timestamp:         time.Now(),
			ComputationTimeNs: elapsed.Nanoseconds(),
			Status:            "error",
			ErrorMessage:      err.Error(),
		})
		return 0, err
	}

	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "power",
		Operands:          []float64{base, exp},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	return result, nil
}

// Factorial computes n! iteratively (avoids stack overflow vs recursive)
func (s *CalculatorService) Factorial(ctx context.Context, requestID string, n int64) (float64, error) {
	start := time.Now()

	if n < 0 {
		err := fmt.Errorf("factorial is not defined for negative numbers: n=%d", n)
		s.publishEvent(ctx, CalculationEvent{
			RequestID:         requestID,
			Operation:         "factorial",
			Operands:          []float64{float64(n)},
			Timestamp:         time.Now(),
			ComputationTimeNs: time.Since(start).Nanoseconds(),
			Status:            "error",
			ErrorMessage:      err.Error(),
		})
		return 0, err
	}

	if n > 170 {
		err := fmt.Errorf("factorial overflow: n=%d exceeds float64 max precision", n)
		return 0, err
	}

	result := 1.0
	for i := int64(2); i <= n; i++ {
		// Check context cancellation for large computations
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		result *= float64(i)
	}

	elapsed := time.Since(start)
	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "factorial",
		Operands:          []float64{float64(n)},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	return result, nil
}

// Fibonacci computes the nth Fibonacci number using memoization
func (s *CalculatorService) Fibonacci(ctx context.Context, requestID string, n int64) (float64, error) {
	start := time.Now()

	if n < 0 {
		return 0, fmt.Errorf("fibonacci is not defined for negative numbers: n=%d", n)
	}

	if n > 1000 {
		return 0, fmt.Errorf("fibonacci n=%d exceeds maximum supported value of 1000", n)
	}

	result, err := fibMemo(ctx, n)
	if err != nil {
		return 0, err
	}

	elapsed := time.Since(start)
	s.publishEvent(ctx, CalculationEvent{
		RequestID:         requestID,
		Operation:         "fibonacci",
		Operands:          []float64{float64(n)},
		Result:            result,
		Timestamp:         time.Now(),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            "success",
	})

	return result, nil
}

// fibMemo uses dynamic programming to compute Fibonacci
func fibMemo(ctx context.Context, n int64) (float64, error) {
	if n <= 1 {
		return float64(n), nil
	}
	prev, curr := 0.0, 1.0
	for i := int64(2); i <= n; i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		prev, curr = curr, prev+curr
	}
	return curr, nil
}

// publishEvent asynchronously publishes a calculation event
func (s *CalculatorService) publishEvent(ctx context.Context, event CalculationEvent) {
	if s.publisher == nil {
		return
	}
	// Fire-and-forget: don't block the RPC response on message publishing
	go func() {
		pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.publisher.Publish(pubCtx, event); err != nil {
			s.log.Warn("failed to publish calculation event",
				zap.String("request_id", event.RequestID),
				zap.String("operation", event.Operation),
				zap.Error(err),
			)
		}
	}()
}
