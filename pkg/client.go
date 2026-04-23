package pkg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"
)

type MessageHandler func(topic string, payload []byte)

type Client struct {
	addr     string
	clientID string
	conn     net.Conn

	mu           sync.RWMutex
	handlers     map[string]MessageHandler
	lastPacketID uint16
	pendingPubs  map[uint16]chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	Log *slog.Logger
}

// addr - адрес брокера, например "localhost:1883"
// id - идентификатор клиента. Если пустой, будет сгенерирован случайный UUID.
func NewClient(addr string, id string, log *slog.Logger) *Client {

	if id == "" {
		id = uuid.New().String()
	}
	return &Client{
		addr:        addr,
		clientID:    id,
		handlers:    make(map[string]MessageHandler),
		pendingPubs: make(map[uint16]chan struct{}),
		Log:         log,
	}
}

func (c *Client) nextPacketID() uint16 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.lastPacketID++
	return c.lastPacketID
}

func (c *Client) Connect() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn

	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Connect,
		},
		ProtocolVersion: 4,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte{0x4d, 0x51, 0x54, 0x54},
			ClientIdentifier: c.clientID,
			Keepalive:        60,
			Clean:            false,
		},
	}

	if err := c.writePacket(pk); err != nil {
		return err
	}

	resp, err := c.readPacket()
	if err != nil {
		return err
	}

	if resp.FixedHeader.Type != packets.Connack {
		return fmt.Errorf("expected CONNACK, got %d", resp.FixedHeader.Type)
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.readLoop()

	return nil
}

func (c *Client) Publish(topic string, payload []byte) error {
	packetID := c.nextPacketID()
	ackChan := make(chan struct{}, 1)

	c.mu.Lock()
	c.pendingPubs[packetID] = ackChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingPubs, packetID)
		c.mu.Unlock()
	}()

	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  1,
		},
		TopicName: topic,
		Payload:   payload,
		PacketID:  packetID,
	}

	if err := c.writePacket(pk); err != nil {
		return err
	}

	select {
	case <-ackChan:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("publish timeout for packet %d", packetID)
	case <-c.ctx.Done():
		return fmt.Errorf("client disconnected")
	}
}

func (c *Client) Subscribe(topic string, handler func(topic string, payload []byte)) error {
	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Subscribe,
			Qos:  1,
		},
		PacketID: c.nextPacketID(),
		Filters: packets.Subscriptions{
			{Filter: topic, Qos: 1},
		},
	}

	if err := c.writePacket(pk); err != nil {
		return err
	}

	c.mu.Lock()
	c.handlers[topic] = handler
	c.mu.Unlock()

	return nil
}

func (c *Client) readLoop() {
	c.Log.Info("Read loop started")
	defer c.Log.Info("Read loop stopped")

	for {
		select {
		case <-c.ctx.Done():
			c.Log.Info("Context canceled, exiting read loop")
			return
		default:

			pk, err := c.readPacket()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || err == io.EOF {
					c.Log.Debug("Connection closed by broker")
					return
				}
				c.Log.Error("Read packet error", "error", err)
				continue
			}

			switch pk.FixedHeader.Type {
			case packets.Publish:
				c.handleIncomingPublish(pk)

			case packets.Puback:
				c.mu.RLock()
				ackChan, ok := c.pendingPubs[pk.PacketID]
				c.mu.RUnlock()

				if ok {
					select {
					case ackChan <- struct{}{}:
					default:
					}
				}
				c.Log.Debug("Received PUBACK from broker", "PacketID", pk.PacketID)

			case packets.Suback:
				c.Log.Debug("Subscription confirmed", "PacketID", pk.PacketID)
			}
		}
	}
}

func (c *Client) handleIncomingPublish(pk *packets.Packet) {
	c.mu.RLock()
	handler, ok := c.handlers[pk.TopicName]
	c.mu.RUnlock()

	if ok && handler != nil {
		go handler(pk.TopicName, pk.Payload)
	}
	if pk.FixedHeader.Qos > 0 {
		ack := &packets.Packet{
			FixedHeader: packets.FixedHeader{
				Type: packets.Puback,
			},
			PacketID: pk.PacketID,
		}

		if err := c.writePacket(ack); err != nil {
			c.Log.Error("Failed to send PUBACK", "error", err)
		}
	}
}

func (c *Client) Disconnect() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
