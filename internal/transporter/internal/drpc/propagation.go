package drpc

import (
	"context"
	"encoding/binary"
	"net"
	"strings"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/internal/transporter/internal/def"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const maxPropagationBytes = 8 << 10

// propagationBuffer snapshots metadata before queueing. Encoding is delayed
// until writing, because reconnecting may select a peer without this capability.
type propagationBuffer struct {
	buffer.Buffer
	metadata []byte
}

func snapshotPropagation(ctx context.Context, frame buffer.Buffer) (buffer.Buffer, error) {
	propagator := otel.GetTextMapPropagator()
	if len(propagator.Fields()) == 0 {
		return frame, nil
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return frame, nil
	}
	if frame.Len() < def.MinFrameSize {
		return nil, errors.ErrInvalidMessage
	}
	metadata, err := json.Marshal(carrier)
	if err != nil {
		return nil, err
	}
	if len(metadata) > maxPropagationBytes || frame.Len()+def.B16+len(metadata) > def.MaxFrameSize {
		return nil, errors.ErrMessageTooLarge
	}
	return &propagationBuffer{Buffer: frame, metadata: metadata}, nil
}

// appendPropagation emits a new header and aliases the original private bytes.
// It never mutates shared payload buffers, including fanout buffers with Delay.
func appendPropagation(parts net.Buffers, frame buffer.Buffer, capable bool) net.Buffers {
	wrapped, ok := frame.(*propagationBuffer)
	if !ok || !capable {
		frame.VisitBytes(func(data []byte) bool { parts = append(parts, data); return true })
		return parts
	}
	header := make([]byte, def.MinFrameSize+def.B16+len(wrapped.metadata))
	copied := 0
	frame.VisitBytes(func(data []byte) bool {
		copied += copy(header[copied:def.MinFrameSize], data)
		return copied < def.MinFrameSize
	})
	binary.BigEndian.PutUint32(header, uint32(frame.Len()-def.SizeBytes+def.B16+len(wrapped.metadata)))
	header[def.SizeBytes] |= def.PropagationMetadataBit
	binary.BigEndian.PutUint16(header[def.MinFrameSize:], uint16(len(wrapped.metadata)))
	copy(header[def.MinFrameSize+def.B16:], wrapped.metadata)
	parts = append(parts, header)
	skip := def.MinFrameSize
	frame.VisitBytes(func(data []byte) bool {
		if skip >= len(data) {
			skip -= len(data)
			return true
		}
		parts = append(parts, data[skip:])
		skip = 0
		return true
	})
	return parts
}

func readPropagation(frame *buffer.Bytes) (propagation.MapCarrier, error) {
	if frame.Len() < def.B16 {
		return nil, errors.ErrInvalidMessage
	}
	data := frame.Bytes()
	size := int(binary.BigEndian.Uint16(data))
	if size == 0 || size > maxPropagationBytes || frame.Len() < def.B16+size {
		return nil, errors.ErrInvalidMessage
	}
	var carrier propagation.MapCarrier
	if err := json.Unmarshal(data[def.B16:def.B16+size], &carrier); err != nil {
		return nil, errors.ErrInvalidMessage
	}
	// A decoder may alias its input. Request buffers are pooled, but context
	// values can outlive them, so keys and values must own their strings.
	owned := make(propagation.MapCarrier, len(carrier))
	for key, value := range carrier {
		owned[strings.Clone(key)] = strings.Clone(value)
	}
	frame.Slide(def.B16 + size)
	return owned, nil
}

func (r *reader) context() context.Context {
	base := context.Background()
	if len(r.carrier) == 0 {
		return base
	}
	return otel.GetTextMapPropagator().Extract(base, r.carrier)
}
