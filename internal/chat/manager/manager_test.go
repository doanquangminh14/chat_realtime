package manager_test

import (
	"sync"
	"testing"

	"github.com/distributed-systems/internal/chat/manager"
	"github.com/distributed-systems/internal/chat/model"
	"github.com/distributed-systems/internal/logger"
)

// mockConn is a test double for manager.ClientConn
type mockConn struct {
	mu       sync.Mutex
	id       string
	username string
	room     string
	sent     []model.Message
}

func (m *mockConn) ID() string       { return m.id }
func (m *mockConn) Username() string { return m.username }
func (m *mockConn) Room() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.room
}
func (m *mockConn) SetRoom(r string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.room = r
}
func (m *mockConn) Send(msg model.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, msg)
	return nil
}
func (m *mockConn) SentMessages() []model.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]model.Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func newMock(id, username string) *mockConn {
	return &mockConn{id: id, username: username}
}

func newManager() *manager.Manager {
	return manager.NewManager(10, logger.NewNop())
}

// --- Tests ---

func TestRegisterUnregister(t *testing.T) {
	mgr := newManager()
	c := newMock("1", "alice")

	mgr.Register(c)
	if mgr.ClientCount() != 1 {
		t.Errorf("expected 1 client, got %d", mgr.ClientCount())
	}

	mgr.Unregister(c)
	if mgr.ClientCount() != 0 {
		t.Errorf("expected 0 clients, got %d", mgr.ClientCount())
	}
}

func TestJoinRoom_BroadcastsJoinNotification(t *testing.T) {
	mgr := newManager()
	alice := newMock("1", "alice")
	bob := newMock("2", "bob")

	mgr.Register(alice)
	mgr.Register(bob)
	mgr.JoinRoom(alice, "general")
	mgr.JoinRoom(bob, "general")

	// bob should receive alice's join + his own join notification? No —
	// JoinRoom broadcasts to existing members (excludes joiner).
	// bob joins after alice, so alice gets bob's join notification.
	msgs := alice.SentMessages()
	if len(msgs) == 0 {
		t.Error("expected alice to receive bob's join notification")
	}
}

func TestBroadcastToRoom(t *testing.T) {
	mgr := newManager()
	alice := newMock("1", "alice")
	bob := newMock("2", "bob")
	charlie := newMock("3", "charlie")

	mgr.Register(alice)
	mgr.Register(bob)
	mgr.Register(charlie)

	mgr.JoinRoom(alice, "general")
	mgr.JoinRoom(bob, "general")
	mgr.JoinRoom(charlie, "other")

	// Clear notifications before the actual test
	alice.mu.Lock()
	alice.sent = nil
	alice.mu.Unlock()
	bob.mu.Lock()
	bob.sent = nil
	bob.mu.Unlock()

	msg := model.NewChatMessage("system", "general", "test broadcast")
	mgr.BroadcastToRoom("general", msg, "")

	if len(alice.SentMessages()) != 1 {
		t.Errorf("alice: expected 1 message, got %d", len(alice.SentMessages()))
	}
	if len(bob.SentMessages()) != 1 {
		t.Errorf("bob: expected 1 message, got %d", len(bob.SentMessages()))
	}
	if len(charlie.SentMessages()) != 0 {
		t.Errorf("charlie: should not receive messages from 'general', got %d", len(charlie.SentMessages()))
	}
}

func TestBroadcastToRoom_ExcludesSender(t *testing.T) {
	mgr := newManager()
	alice := newMock("1", "alice")
	bob := newMock("2", "bob")

	mgr.Register(alice)
	mgr.Register(bob)
	mgr.JoinRoom(alice, "general")
	mgr.JoinRoom(bob, "general")

	// Clear
	alice.mu.Lock()
	alice.sent = nil
	alice.mu.Unlock()
	bob.mu.Lock()
	bob.sent = nil
	bob.mu.Unlock()

	msg := model.NewChatMessage("alice", "general", "hello")
	mgr.BroadcastToRoom("general", msg, alice.ID()) // exclude alice

	if len(alice.SentMessages()) != 0 {
		t.Error("alice should not receive her own broadcast")
	}
	if len(bob.SentMessages()) != 1 {
		t.Errorf("bob: expected 1 message, got %d", len(bob.SentMessages()))
	}
}

func TestIsUsernameTaken(t *testing.T) {
	mgr := newManager()
	alice := newMock("1", "alice")
	mgr.Register(alice)

	if !mgr.IsUsernameTaken("alice") {
		t.Error("expected alice to be taken")
	}
	if mgr.IsUsernameTaken("bob") {
		t.Error("bob should not be taken")
	}
}

func TestListRooms(t *testing.T) {
	mgr := newManager()
	a := newMock("1", "a")
	b := newMock("2", "b")
	c := newMock("3", "c")

	mgr.Register(a)
	mgr.Register(b)
	mgr.Register(c)

	mgr.JoinRoom(a, "general")
	mgr.JoinRoom(b, "general")
	mgr.JoinRoom(c, "offtopic")

	rooms := mgr.ListRooms()
	if rooms["general"] != 2 {
		t.Errorf("general: expected 2 members, got %d", rooms["general"])
	}
	if rooms["offtopic"] != 1 {
		t.Errorf("offtopic: expected 1 member, got %d", rooms["offtopic"])
	}
}

func TestLeaveRoom_CleansUpEmptyRoom(t *testing.T) {
	mgr := newManager()
	a := newMock("1", "a")
	mgr.Register(a)
	mgr.JoinRoom(a, "temp")

	if len(mgr.ListRooms()) != 1 {
		t.Error("expected 1 room")
	}

	mgr.LeaveRoom(a)
	if len(mgr.ListRooms()) != 0 {
		t.Error("expected room to be deleted when empty")
	}
}

func TestConcurrentRegister(t *testing.T) {
	mgr := newManager()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			c := newMock(string(rune('a'+n%26))+"-"+string(rune('0'+n%10)), "user")
			mgr.Register(c)
			mgr.JoinRoom(c, "stress")
			mgr.Unregister(c)
		}(i)
	}
	wg.Wait()
}
