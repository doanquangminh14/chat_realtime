package model

import "time"

// MessageType defines the type of a chat message
type MessageType string

const (
	MessageTypeChat    MessageType = "chat"
	MessageTypeSystem  MessageType = "system"
	MessageTypePrivate MessageType = "private"
	MessageTypePing    MessageType = "ping"
	MessageTypePong    MessageType = "pong"
	MessageTypeJoin    MessageType = "join"
	MessageTypeLeave   MessageType = "leave"
	MessageTypeHistory MessageType = "history"
	MessageTypeRooms   MessageType = "rooms"
	MessageTypeError   MessageType = "error"
)

// Message represents a chat protocol message
type Message struct {
	Type      MessageType `json:"type"`
	From      string      `json:"from,omitempty"`
	To        string      `json:"to,omitempty"` // for private messages
	Room      string      `json:"room,omitempty"`
	Content   string      `json:"content"`
	Timestamp time.Time   `json:"timestamp"`
}

// NewSystemMessage creates a system notification message
func NewSystemMessage(content string) Message {
	return Message{
		Type:      MessageTypeSystem,
		Content:   content,
		Timestamp: time.Now(),
	}
}

// NewChatMessage creates a chat message from a user
func NewChatMessage(from, room, content string) Message {
	return Message{
		Type:      MessageTypeChat,
		From:      from,
		Room:      room,
		Content:   content,
		Timestamp: time.Now(),
	}
}

// NewPrivateMessage creates a private message
func NewPrivateMessage(from, to, content string) Message {
	return Message{
		Type:      MessageTypePrivate,
		From:      from,
		To:        to,
		Content:   content,
		Timestamp: time.Now(),
	}
}

// NewErrorMessage creates an error message
func NewErrorMessage(content string) Message {
	return Message{
		Type:      MessageTypeError,
		Content:   content,
		Timestamp: time.Now(),
	}
}

// Client represents a connected chat client state
type Client struct {
	ID       string
	Username string
	Room     string
	JoinedAt time.Time
}
