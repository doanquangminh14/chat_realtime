package utils

import "github.com/google/uuid"

// NewRequestID generates a new UUID v4 string suitable for tracing.
func NewRequestID() string {
	return uuid.New().String()
}

// ShortID returns the first 8 characters of a UUID for compact display.
func ShortID(id string) string {
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}
