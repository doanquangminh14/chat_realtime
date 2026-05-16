package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/distributed-systems/internal/chat/manager"
	"github.com/distributed-systems/internal/chat/model"
	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ChatServer listens for TCP connections and manages client sessions
type ChatServer struct {
	cfg      config.ChatConfig
	manager  *manager.Manager
	log      *logger.Logger
	listener net.Listener
}

// NewChatServer creates a new chat server
func NewChatServer(cfg config.ChatConfig, mgr *manager.Manager, log *logger.Logger) *ChatServer {
	return &ChatServer{
		cfg:     cfg,
		manager: mgr,
		log:     log.WithComponent("chat-server"),
	}
}

// Start begins listening for connections
func (s *ChatServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Address())
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.Address(), err)
	}
	s.listener = ln

	s.log.Info("chat server started", zap.String("address", s.cfg.Address()))

	go func() {
		<-ctx.Done()
		s.log.Info("chat server shutting down")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				s.log.Error("accept error", zap.Error(err))
				continue
			}
		}

		if s.manager.ClientCount() >= s.cfg.MaxClients {
			s.log.Warn("max clients reached, rejecting connection")
			_ = conn.Close()
			continue
		}

		go s.handleConn(ctx, conn)
	}
}

// handleConn manages the lifecycle of a single client connection
func (s *ChatServer) handleConn(ctx context.Context, netConn net.Conn) {
	clientConn := newClientConn(netConn, s.cfg, s.log)
	defer clientConn.close()

	// Handshake: ask for username
	if err := clientConn.sendRaw("Welcome! Enter your username: "); err != nil {
		s.log.Warn("failed to send welcome", zap.Error(err))
		return
	}

	username, err := clientConn.readLine()
	if err != nil {
		return
	}
	username = strings.TrimSpace(username)
	if username == "" {
		_ = clientConn.Send(model.NewErrorMessage("username cannot be empty"))
		return
	}
	if s.manager.IsUsernameTaken(username) {
		_ = clientConn.Send(model.NewErrorMessage("username already taken"))
		return
	}

	clientConn.setUsername(username)
	s.manager.Register(clientConn)
	defer s.manager.Unregister(clientConn)

	s.log.Info("client connected",
		zap.String("client_id", clientConn.ID()),
		zap.String("username", username),
		zap.String("remote_addr", netConn.RemoteAddr().String()),
	)

	// Send instructions
	_ = clientConn.Send(model.NewSystemMessage(
		"Commands: /join <room>, /leave, /rooms, /msg <user> <text>, /quit",
	))

	// Heartbeat goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(s.cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := clientConn.Send(model.Message{Type: model.MessageTypePing, Timestamp: time.Now()}); err != nil {
					return
				}
			}
		}
	}()

	// Read loop
	scanner := bufio.NewScanner(netConn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !s.handleMessage(clientConn, line) {
			break
		}
	}

	wg.Wait()
}

func (s *ChatServer) handleMessage(conn *clientConn, line string) bool {

	// Ignore heartbeat pong messages
	if strings.Contains(line, `"type":"pong"`) {
		return true
	}

	// Handle slash commands
	if strings.HasPrefix(line, "/") {
		return s.handleCommand(conn, line)
	}

	// User must join room first
	if conn.Room() == "" {
		_ = conn.Send(
			model.NewErrorMessage(
				"join a room first with /join <room>",
			),
		)
		return true
	}

	// Broadcast normal chat message
	msg := model.NewChatMessage(
		conn.Username(),
		conn.Room(),
		line,
	)

	s.manager.BroadcastToRoom(
		conn.Room(),
		msg,
		"",
	)

	return true
}

// handleCommand processes slash commands
func (s *ChatServer) handleCommand(conn *clientConn, line string) bool {
	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "/join":
		if len(parts) < 2 {
			_ = conn.Send(model.NewErrorMessage("usage: /join <room>"))
			return true
		}

		room := parts[1]

		conn.SetRoom(room)

		history := s.manager.JoinRoom(conn, room)

		// Send history to newly joined client
		for _, msg := range history {
			_ = conn.Send(msg)
		}

		_ = conn.Send(
			model.NewSystemMessage(
				fmt.Sprintf("joined room: %s", room),
			),
		)

	case "/leave":
		s.manager.LeaveRoom(conn)
		_ = conn.Send(model.NewSystemMessage("left the room"))

	case "/rooms":
		rooms := s.manager.ListRooms()
		content := "Active rooms:\n"
		for room, count := range rooms {
			content += fmt.Sprintf("  #%s (%d members)\n", room, count)
		}
		if len(rooms) == 0 {
			content = "No active rooms"
		}
		_ = conn.Send(model.NewSystemMessage(content))

	case "/msg":
		if len(parts) < 3 {
			_ = conn.Send(model.NewErrorMessage("usage: /msg <user> <text>"))
			return true
		}
		toUser := parts[1]
		text := strings.Join(parts[2:], " ")
		msg := model.NewPrivateMessage(conn.Username(), toUser, text)
		if err := s.manager.SendPrivate(conn, toUser, msg); err != nil {
			_ = conn.Send(model.NewErrorMessage("failed to send private message"))
		} else {
			// Echo back to sender
			_ = conn.Send(msg)
		}

	case "/quit":
		return false

	default:
		_ = conn.Send(model.NewErrorMessage("unknown command: " + cmd))
	}

	return true
}

// --- clientConn wraps a net.Conn with chat state ---

type clientConn struct {
	id       string
	username string
	room     string
	mu       sync.RWMutex
	conn     net.Conn
	writer   *bufio.Writer
	cfg      config.ChatConfig
	log      *logger.Logger
}

func newClientConn(conn net.Conn, cfg config.ChatConfig, log *logger.Logger) *clientConn {
	return &clientConn{
		id:     uuid.New().String(),
		conn:   conn,
		writer: bufio.NewWriter(conn),
		cfg:    cfg,
		log:    log,
	}
}

func (c *clientConn) ID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

func (c *clientConn) Username() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.username
}

func (c *clientConn) Room() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.room
}

func (c *clientConn) SetRoom(room string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.room = room
}

func (c *clientConn) setUsername(username string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.username = username
}

func (c *clientConn) Send(msg model.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	_, err = c.writer.Write(append(data, '\n'))
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *clientConn) sendRaw(text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := fmt.Fprint(c.conn, text)
	return err
}

func (c *clientConn) readLine() (string, error) {

	reader := bufio.NewReader(c.conn)

	return reader.ReadString('\n')
}

func (c *clientConn) close() {
	_ = c.conn.Close()
}
