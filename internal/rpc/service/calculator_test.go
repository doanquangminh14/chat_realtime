package service_test

import (
	"context"
	"testing"

	"github.com/distributed-systems/internal/logger"
	"github.com/distributed-systems/internal/rpc/service"
)

// noopPublisher satisfies service.EventPublisher without RabbitMQ
type noopPublisher struct{}

func (n *noopPublisher) Publish(_ context.Context, _ service.CalculationEvent) error {
	return nil
}

func newTestService() *service.CalculatorService {
	log := logger.NewNop()
	return service.NewCalculatorService(log, &noopPublisher{})
}

// --- Add ---

func TestAdd(t *testing.T) {
	svc := newTestService()
	tests := []struct {
		name     string
		a, b     float64
		expected float64
	}{
		{"positive", 10, 5, 15},
		{"negative", -3, -7, -10},
		{"mixed", 100, -50, 50},
		{"zeros", 0, 0, 0},
		{"floats", 1.1, 2.2, 3.3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.Add(context.Background(), "test-req", tt.a, tt.b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if abs(result-tt.expected) > 1e-9 {
				t.Errorf("Add(%v, %v) = %v; want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// --- Subtract ---

func TestSubtract(t *testing.T) {
	svc := newTestService()
	result, err := svc.Subtract(context.Background(), "req", 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if result != 7 {
		t.Errorf("expected 7, got %v", result)
	}
}

// --- Multiply ---

func TestMultiply(t *testing.T) {
	svc := newTestService()
	tests := []struct{ a, b, want float64 }{
		{3, 4, 12},
		{-2, 5, -10},
		{0, 999, 0},
		{1.5, 2, 3},
	}
	for _, tt := range tests {
		result, err := svc.Multiply(context.Background(), "req", tt.a, tt.b)
		if err != nil {
			t.Fatal(err)
		}
		if abs(result-tt.want) > 1e-9 {
			t.Errorf("Multiply(%v, %v) = %v; want %v", tt.a, tt.b, result, tt.want)
		}
	}
}

// --- Divide ---

func TestDivide_Success(t *testing.T) {
	svc := newTestService()
	result, err := svc.Divide(context.Background(), "req", 10, 4)
	if err != nil {
		t.Fatal(err)
	}
	if abs(result-2.5) > 1e-9 {
		t.Errorf("expected 2.5, got %v", result)
	}
}

func TestDivide_ByZero(t *testing.T) {
	svc := newTestService()
	_, err := svc.Divide(context.Background(), "req", 10, 0)
	if err == nil {
		t.Fatal("expected error for division by zero, got nil")
	}
}

// --- Power ---

func TestPower(t *testing.T) {
	svc := newTestService()
	tests := []struct{ base, exp, want float64 }{
		{2, 10, 1024},
		{3, 3, 27},
		{5, 0, 1},
		{2, -1, 0.5},
	}
	for _, tt := range tests {
		result, err := svc.Power(context.Background(), "req", tt.base, tt.exp)
		if err != nil {
			t.Fatalf("Power(%v, %v) unexpected error: %v", tt.base, tt.exp, err)
		}
		if abs(result-tt.want) > 1e-9 {
			t.Errorf("Power(%v, %v) = %v; want %v", tt.base, tt.exp, result, tt.want)
		}
	}
}

// --- Factorial ---

func TestFactorial(t *testing.T) {
	svc := newTestService()
	tests := []struct {
		n    int64
		want float64
	}{
		{0, 1},
		{1, 1},
		{5, 120},
		{10, 3628800},
		{20, 2432902008176640000},
	}
	for _, tt := range tests {
		result, err := svc.Factorial(context.Background(), "req", tt.n)
		if err != nil {
			t.Fatalf("Factorial(%d) unexpected error: %v", tt.n, err)
		}
		if abs(result-tt.want) > 1 { // allow float64 epsilon for large values
			t.Errorf("Factorial(%d) = %v; want %v", tt.n, result, tt.want)
		}
	}
}

func TestFactorial_Negative(t *testing.T) {
	svc := newTestService()
	_, err := svc.Factorial(context.Background(), "req", -1)
	if err == nil {
		t.Fatal("expected error for negative factorial")
	}
}

func TestFactorial_Overflow(t *testing.T) {
	svc := newTestService()
	_, err := svc.Factorial(context.Background(), "req", 200)
	if err == nil {
		t.Fatal("expected overflow error for n=200")
	}
}

// --- Fibonacci ---

func TestFibonacci(t *testing.T) {
	svc := newTestService()
	tests := []struct {
		n    int64
		want float64
	}{
		{0, 0},
		{1, 1},
		{2, 1},
		{10, 55},
		{20, 6765},
		{50, 12586269025},
	}
	for _, tt := range tests {
		result, err := svc.Fibonacci(context.Background(), "req", tt.n)
		if err != nil {
			t.Fatalf("Fibonacci(%d) unexpected error: %v", tt.n, err)
		}
		if abs(result-tt.want) > 1 {
			t.Errorf("Fibonacci(%d) = %v; want %v", tt.n, result, tt.want)
		}
	}
}

func TestFibonacci_Negative(t *testing.T) {
	svc := newTestService()
	_, err := svc.Fibonacci(context.Background(), "req", -5)
	if err == nil {
		t.Fatal("expected error for negative fibonacci")
	}
}

func TestFibonacci_TooLarge(t *testing.T) {
	svc := newTestService()
	_, err := svc.Fibonacci(context.Background(), "req", 1001)
	if err == nil {
		t.Fatal("expected error for n > 1000")
	}
}

// --- Context cancellation ---

func TestFactorial_ContextCancelled(t *testing.T) {
	svc := newTestService()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	_, err := svc.Factorial(ctx, "req", 100)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

// --- Helper ---

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
