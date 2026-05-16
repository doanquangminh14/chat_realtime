package shared_test

import (
	"errors"
	"testing"

	"github.com/distributed-systems/internal/shared"
)

func TestDomainError_Error(t *testing.T) {
	err := &shared.DomainError{
		Code:    "TEST_CODE",
		Message: "something went wrong",
	}
	got := err.Error()
	expected := "[TEST_CODE] something went wrong"
	if got != expected {
		t.Errorf("got %q; want %q", got, expected)
	}
}

func TestDomainError_WithWrappedError(t *testing.T) {
	inner := errors.New("inner cause")
	err := &shared.DomainError{
		Code:    "WRAP",
		Message: "wrapped",
		Err:     inner,
	}
	if !errors.Is(err, inner) {
		t.Error("errors.Is should find inner error via Unwrap")
	}
}

func TestNewInvalidInputError(t *testing.T) {
	err := shared.NewInvalidInputError("field required")
	if err.Code != shared.ErrCodeInvalidInput {
		t.Errorf("expected code %s, got %s", shared.ErrCodeInvalidInput, err.Code)
	}
}

func TestNewDivisionByZeroError(t *testing.T) {
	err := shared.NewDivisionByZeroError()
	if err.Code != shared.ErrCodeDivisionByZero {
		t.Errorf("expected code %s, got %s", shared.ErrCodeDivisionByZero, err.Code)
	}
}
