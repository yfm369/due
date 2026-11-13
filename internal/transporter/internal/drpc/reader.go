package drpc

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/internal/transporter/internal/def"
	"go.opentelemetry.io/otel/propagation"
)

type reader struct {
	reader  *bufio.Reader
	header  uint8
	carrier propagation.MapCarrier
}

func newReader(conn *net.TCPConn) *reader {
	return &reader{reader: bufio.NewReaderSize(conn, 1<<16)}
}

// read reads a message into a buffer.
func (r *reader) read() (bool, uint8, uint64, *buffer.Bytes, error) {
	r.header, r.carrier = 0, nil
	// Parse the header with a zero-copy Peek, avoiding two io.ReadFull calls per frame.
	header, err := r.reader.Peek(def.SizeBytes + def.HeaderBytes)
	if err != nil {
		return false, 0, 0, nil, err
	}

	size := binary.BigEndian.Uint32(header[:def.SizeBytes])
	r.header = header[def.SizeBytes]

	if header[def.SizeBytes]&def.HeartbeatBit == def.HeartbeatBit {
		if size != def.HeaderBytes || r.header&def.PropagationMetadataBit != 0 {
			return false, 0, 0, nil, errors.ErrInvalidMessage
		}

		_, _ = r.reader.Discard(def.SizeBytes + def.HeaderBytes)

		return true, 0, 0, nil, nil
	}

	if size < def.MinFrameSize-def.SizeBytes {
		return false, 0, 0, nil, errors.ErrInvalidMessage
	}

	if uint64(size)+def.SizeBytes > def.MaxFrameSize {
		return false, 0, 0, nil, errors.ErrMessageTooLarge
	}

	buf := buffer.MallocBytes(int(size) - def.HeaderBytes)

	_, _ = r.reader.Discard(def.SizeBytes + def.HeaderBytes)

	if _, err := io.ReadFull(r.reader, buf.Bytes()); err != nil {
		buf.Release()
		return false, 0, 0, nil, err
	}

	var (
		data  = buf.Bytes()
		route = data[:def.RouteBytes][0]
		seq   = binary.BigEndian.Uint64(data[def.RouteBytes : def.RouteBytes+def.SeqBytes])
	)

	buf.Slide(def.RouteBytes + def.SeqBytes)
	if r.header&def.PropagationMetadataBit != 0 {
		r.carrier, err = readPropagation(buf)
		if err != nil {
			buf.Release()
			return false, 0, 0, nil, err
		}
	}

	return false, route, seq, buf, nil
}
