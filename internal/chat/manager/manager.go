package manager

import (
	"sync"
	"time"

	"github.com/distributed-systems/internal/chat/model"
	"github.com/distributed-systems/internal/logger"
	"go.uber.org/zap"
)

// ClientConn defines the interface for sending messages to a client connection
type ClientConn interface {
	Send(msg model.Message) error
	ID() string
	Username() string
	Room() string
	SetRoom(room string)
}

// RoomHistory holds the recent message history for a room
type RoomHistory struct {
	mu       sync.RWMutex
	messages []model.Message
	limit    int
}

func newRoomHistory(limit int) *RoomHistory {
	return &RoomHistory{
		messages: make([]model.Message, 0, limit),
		limit:    limit,
	}
}

func (h *RoomHistory) add(msg model.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.messages) >= h.limit {
		h.messages = h.messages[1:]
	}
	h.messages = append(h.messages, msg)
}

func (h *RoomHistory) get() []model.Message {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([]model.Message, len(h.messages))
	copy(result, h.messages)
	return result
}

// Manager manages all connected clients, rooms and broadcasts
type Manager struct {
	mu            sync.RWMutex
	clients       map[string]ClientConn            // clientID -> conn
	rooms         map[string]map[string]ClientConn // room -> clientID -> conn
	roomHistories map[string]*RoomHistory
	log           *logger.Logger
	historyLimit  int
}

// NewManager creates a new client manager
func NewManager(historyLimit int, log *logger.Logger) *Manager {
	return &Manager{
		clients:       make(map[string]ClientConn),
		rooms:         make(map[string]map[string]ClientConn),
		roomHistories: make(map[string]*RoomHistory),
		log:           log.WithComponent("chat-manager"),
		historyLimit:  historyLimit,
	}
}

// Register adds a client to the manager
func (m *Manager) Register(conn ClientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[conn.ID()] = conn
	m.log.Info("client registered",
		zap.String("client_id", conn.ID()),
		zap.Int("total_clients", len(m.clients)),
	)
}

// Unregister removes a client and cleans up room membership
func (m *Manager) Unregister(conn ClientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.clients, conn.ID())

	// Remove from room
	room := conn.Room()
	if room != "" {
		if members, ok := m.rooms[room]; ok {
			delete(members, conn.ID())
			if len(members) == 0 {
				delete(m.rooms, room)
				m.log.Info("room closed (empty)", zap.String("room", room))
			}
		}
	}

	m.log.Info("client unregistered",
		zap.String("client_id", conn.ID()),
		zap.String("username", conn.Username()),
		zap.Int("total_clients", len(m.clients)),
	)
}

// JoinRoom moves a client to a room
func (m *Manager) JoinRoom(conn ClientConn, room string) []model.Message {
	m.mu.Lock()

	// Leave old room
	oldRoom := conn.Room()
	if oldRoom != "" && oldRoom != room {
		if members, ok := m.rooms[oldRoom]; ok {
			delete(members, conn.ID())
		}
	}

	// Join new room
	if _, ok := m.rooms[room]; !ok {
		m.rooms[room] = make(map[string]ClientConn)
	}
	m.rooms[room][conn.ID()] = conn
	conn.SetRoom(room)

	// Get history
	if _, ok := m.roomHistories[room]; !ok {
		m.roomHistories[room] = newRoomHistory(m.historyLimit)
	}
	history := m.roomHistories[room].get()

	m.mu.Unlock()

	// Broadcast join notification
	joinMsg := model.Message{
		Type:      model.MessageTypeSystem,
		Room:      room,
		Content:   conn.Username() + " joined the room",
		Timestamp: time.Now(),
	}
	m.BroadcastToRoom(room, joinMsg, conn.ID())

	m.log.Info("client joined room",
		zap.String("username", conn.Username()),
		zap.String("room", room),
	)

	return history
}

// LeaveRoom removes a client from their current room
func (m *Manager) LeaveRoom(conn ClientConn) {
	m.mu.Lock()
	room := conn.Room()
	if room == "" {
		m.mu.Unlock()
		return
	}
	if members, ok := m.rooms[room]; ok {
		delete(members, conn.ID())
		if len(members) == 0 {
			delete(m.rooms, room)
		}
	}
	conn.SetRoom("")
	m.mu.Unlock()

	leaveMsg := model.Message{
		Type:      model.MessageTypeSystem,
		Room:      room,
		Content:   conn.Username() + " left the room",
		Timestamp: time.Now(),
	}
	m.BroadcastToRoom(room, leaveMsg, conn.ID())
}

// BroadcastToRoom sends a message to all clients in a room except excludeID
func (m *Manager) BroadcastToRoom(room string, msg model.Message, excludeID string) {
	m.mu.RLock()
	members, ok := m.rooms[room]
	if !ok {
		m.mu.RUnlock()
		return
	}
	// Snapshot to avoid holding lock during sends
	targets := make([]ClientConn, 0, len(members))
	for id, conn := range members {
		if id != excludeID {
			targets = append(targets, conn)
		}
	}
	m.mu.RUnlock()

	// Store in history (only regular chat messages)
	if msg.Type == model.MessageTypeChat || msg.Type == model.MessageTypeSystem {
		m.mu.RLock()
		h := m.roomHistories[room]
		m.mu.RUnlock()
		if h != nil {
			h.add(msg)
		}
	}

	for _, conn := range targets {
		if err := conn.Send(msg); err != nil {
			m.log.Warn("failed to send message to client",
				zap.String("client_id", conn.ID()),
				zap.Error(err),
			)
		}
	}
}

// SendPrivate sends a private message to a specific user
func (m *Manager) SendPrivate(fromConn ClientConn, toUsername string, msg model.Message) error {
	m.mu.RLock()
	var target ClientConn
	for _, conn := range m.clients {
		if conn.Username() == toUsername {
			target = conn
			break
		}
	}
	m.mu.RUnlock()

	if target == nil {
		return nil // User not found — caller handles
	}
	return target.Send(msg)
}

// ListRooms returns the names of all active rooms and their member counts
func (m *Manager) ListRooms() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]int, len(m.rooms))
	for room, members := range m.rooms {
		result[room] = len(members)
	}
	return result
}

// ClientCount returns the total number of connected clients
func (m *Manager) ClientCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.clients)
}

// IsUsernameTaken checks if a username is already in use
func (m *Manager) IsUsernameTaken(username string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, conn := range m.clients {
		if conn.Username() == username {
			return true
		}
	}
	return false
}
