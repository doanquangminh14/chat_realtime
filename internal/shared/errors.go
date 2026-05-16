// Package shared contains types shared across multiple internal packages.
package shared

import "fmt"

// DomainError represents a structured error with a code and human-readable message.
type DomainError struct {
	Code    string
	Message string
	Err     error
}

func (e *DomainError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *DomainError) Unwrap() error { return e.Err }

// Common error codes
const (
	ErrCodeInvalidInput   = "INVALID_INPUT"
	ErrCodeDivisionByZero = "DIVISION_BY_ZERO"
	ErrCodeOverflow       = "OVERFLOW"
	ErrCodeNotFound       = "NOT_FOUND"
	ErrCodeInternal       = "INTERNAL"
)

// NewInvalidInputError creates a validation error.
func NewInvalidInputError(msg string) *DomainError {
	return &DomainError{Code: ErrCodeInvalidInput, Message: msg}
}

// NewDivisionByZeroError creates a division-by-zero error.
func NewDivisionByZeroError() *DomainError {
	return &DomainError{Code: ErrCodeDivisionByZero, Message: "division by zero is undefined"}
}
