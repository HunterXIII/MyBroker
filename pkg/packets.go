package pkg

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/mochi-mqtt/server/v2/packets"
)

func (c *Client) writePacket(pk *packets.Packet) error {
	body := new(bytes.Buffer)
	var err error

	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(body)
	case packets.Publish:
		err = pk.PublishEncode(body)
	case packets.Subscribe:
		err = pk.SubscribeEncode(body)
	case packets.Puback:
		err = pk.PubackEncode(body)
	default:
		return fmt.Errorf("unknown packet type: %d", pk.FixedHeader.Type)
	}

	if err != nil {
		return err
	}

	_, err = c.conn.Write(body.Bytes())
	return err
}

func (c *Client) readPacket() (*packets.Packet, error) {
	reader := bufio.NewReader(c.conn)

	headerByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	pk := &packets.Packet{}
	pk.FixedHeader.Decode(headerByte)

	remLen, _, err := packets.DecodeLength(reader)
	if err != nil {
		return nil, err
	}

	body := make([]byte, remLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}

	switch pk.FixedHeader.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Puback:
		err = pk.PubackDecode(body)
	}

	return pk, err
}
