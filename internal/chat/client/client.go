package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/distributed-systems/internal/chat/model"
	"github.com/distributed-systems/internal/logger"
	"go.uber.org/zap"
)

// ChatClient handles TCP chat client operations
type ChatClient struct {
	address string
	log     *logger.Logger
	conn    net.Conn
}

// NewChatClient creates a new chat client
func NewChatClient(address string, log *logger.Logger) *ChatClient {
	return &ChatClient{
		address: address,
		log:     log.WithComponent("chat-client"),
	}
}

// Connect connects to the chat server
func (c *ChatClient) Connect() error {

	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		return fmt.Errorf(
			"connection failed: %w",
			err,
		)
	}

	c.conn = conn

	c.log.Info(
		"connected to chat server",
		zap.String("address", c.address),
	)

	return nil
}

// Run starts the chat client
func (c *ChatClient) Run() error {

	defer c.conn.Close()

	serverReader := bufio.NewReader(c.conn)

	// Read welcome message
	welcome, err := serverReader.ReadString(':')
	if err != nil {
		return fmt.Errorf(
			"failed to read welcome: %w",
			err,
		)
	}

	fmt.Print(welcome, " ")

	// Read username from terminal
	stdinReader := bufio.NewReader(os.Stdin)

	username, err := stdinReader.ReadString('\n')
	if err != nil {
		return err
	}

	username = strings.TrimSpace(username)

	_, err = fmt.Fprintln(c.conn, username)
	if err != nil {
		return err
	}

	fmt.Printf(
		"Connected as [%s]. Type /help for commands.\n",
		username,
	)

	// Start receiving messages
	go c.receiveLoop(serverReader)

	// Start sending messages
	c.sendLoop(stdinReader)

	return nil
}

// receiveLoop receives messages from server
func (c *ChatClient) receiveLoop(reader *bufio.Reader) {

	for {

		line, err := reader.ReadString('\n')
		if err != nil {

			fmt.Println("\n[Disconnected from server]")

			os.Exit(0)
		}

		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		var msg model.Message

		err = json.Unmarshal(
			[]byte(line),
			&msg,
		)

		if err != nil {

			// Fallback raw output
			fmt.Printf("\n%s\n", line)

			continue
		}

		c.displayMessage(msg)
	}
}

func (c *ChatClient) sendLoop(_ *bufio.Reader) {

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("> ")

	for scanner.Scan() {

		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			fmt.Print("> ")
			continue
		}

		_, err := fmt.Fprintln(c.conn, line)
		if err != nil {

			fmt.Println("\n[Error sending message]")

			return
		}

		if strings.ToLower(line) == "/quit" {
			return
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("stdin error:", err)
	}
}

// displayMessage displays formatted messages
func (c *ChatClient) displayMessage(msg model.Message) {

	ts := msg.Timestamp.Format("15:04:05")

	switch msg.Type {

	case model.MessageTypeChat:

		if msg.Room != "" {

			fmt.Printf(
				"\n[%s] #%s | %s: %s\n",
				ts,
				msg.Room,
				msg.From,
				msg.Content,
			)

		} else {

			fmt.Printf(
				"\n[%s] %s: %s\n",
				ts,
				msg.From,
				msg.Content,
			)
		}

	case model.MessageTypePrivate:

		fmt.Printf(
			"\n[%s] 🔒 [PM from %s]: %s\n",
			ts,
			msg.From,
			msg.Content,
		)

	case model.MessageTypeSystem:

		fmt.Printf(
			"\n[%s] *** %s ***\n",
			ts,
			msg.Content,
		)

	case model.MessageTypeError:

		fmt.Printf(
			"\n[%s] ⚠ ERROR: %s\n",
			ts,
			msg.Content,
		)

	case model.MessageTypePing:

		// Ignore heartbeat ping
		return

	default:
		return
	}
}

// Close closes connection
func (c *ChatClient) Close() {

	if c.conn != nil {
		c.conn.Close()
	}
}