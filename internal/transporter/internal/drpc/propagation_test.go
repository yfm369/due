package drpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/internal/transporter/internal/codes"
	"github.com/dobyte/due/v2/internal/transporter/internal/def"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
	"github.com/dobyte/due/v2/internal/transporter/internal/route"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func propagationContext(t *testing.T) context.Context {
	t.Helper()
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	id, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	span, _ := trace.SpanIDFromHex("0123456789abcdef")
	state, _ := trace.ParseTraceState("vendor=due")
	member, _ := baggage.NewMember("test-key", "preserved")
	bag, _ := baggage.New(member)
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: span, TraceFlags: trace.FlagsSampled, TraceState: state}))
}

func TestClientAutomaticallyPropagatesContextAndIsolatesMessages(t *testing.T) {
	parent := propagationContext(t)
	seen := make(chan context.Context, 2)
	server, err := NewServer(&ServerOptions{Addr: listenFreeAddr(t), WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterContextHandler(route.SetState, func(ctx context.Context, conn *ServerConn, seq uint64, request *buffer.Bytes) error {
		_, err := protocol.DecodeSetStateReq(request)
		request.Release()
		if err != nil {
			return err
		}
		seen <- ctx
		return conn.Push(protocol.EncodeSetStateRes(seq, codes.OK))
	})
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	client := newTestDRPCClient(t, server.ListenAddr(), nil)
	for i, ctx := range []context.Context{parent, context.Background()} {
		response, err := client.Call(ctx, uint64(i+2), protocol.EncodeSetStateReq(uint64(i+2), cluster.Work))
		if err != nil {
			t.Fatal(err)
		}
		_, err = protocol.DecodeSetStateRes(response)
		response.Release()
		if err != nil {
			t.Fatal(err)
		}
		incoming := <-seen
		sc := trace.SpanContextFromContext(incoming)
		if i == 0 {
			upstream := trace.SpanContextFromContext(parent)
			if sc.TraceID() != upstream.TraceID() || sc.SpanID() != upstream.SpanID() || !sc.IsRemote() || !sc.IsSampled() || sc.TraceState().String() != upstream.TraceState().String() || baggage.FromContext(incoming).Member("test-key").Value() != "preserved" {
				t.Fatalf("automatic propagation lost context: %v", sc)
			}
		} else if sc.IsValid() || baggage.FromContext(incoming).Len() != 0 {
			t.Fatal("plain request inherited previous message context")
		}
	}
}

// readLegacyFrame implements the previous release's wire layout, deliberately
// ignoring every header flag except heartbeat and leaving private bytes intact.
func readLegacyFrame(conn net.Conn) (byte, byte, uint64, []byte, error) {
	sizeBytes := make([]byte, def.SizeBytes)
	if _, err := io.ReadFull(conn, sizeBytes); err != nil {
		return 0, 0, 0, nil, err
	}
	size := binary.BigEndian.Uint32(sizeBytes)
	if size < def.HeaderBytes+def.RouteBytes+def.SeqBytes || size > def.MaxFrameSize {
		return 0, 0, 0, nil, errors.ErrInvalidMessage
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return 0, 0, 0, nil, err
	}
	return data[0], data[1], binary.BigEndian.Uint64(data[2:10]), data[10:], nil
}

func TestNewClientFallsBackToLegacyServerFraming(t *testing.T) {
	ctx := propagationContext(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	result := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		_, rt, seq, private, err := readLegacyFrame(conn)
		if err != nil || rt != route.Handshake {
			result <- errors.New("invalid legacy handshake request")
			return
		}
		if _, _, _, err := protocol.DecodeHandshakeReq(buffer.NewBytes(private)); err != nil {
			result <- err
			return
		}
		response := protocol.EncodeHandshakeRes(seq, codes.OK)
		_, err = conn.Write(response.Bytes())
		response.Release()
		if err != nil {
			result <- err
			return
		}
		header, rt, seq, private, err := readLegacyFrame(conn)
		if err != nil || header != def.DataBit || rt != route.SetState || !bytes.Equal(private, []byte{byte(cluster.Busy)}) {
			result <- errors.New("new client changed legacy private message framing")
			return
		}
		response = protocol.EncodeSetStateRes(seq, codes.OK)
		_, err = conn.Write(response.Bytes())
		response.Release()
		result <- err
	}()
	client := newTestDRPCClient(t, listener.Addr().String(), nil)
	response, err := client.Call(ctx, 2, protocol.EncodeSetStateReq(2, cluster.Busy))
	if err != nil {
		t.Fatal(err)
	}
	response.Release()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestLegacyClientUsesUnchangedHandshakeAndDataFrames(t *testing.T) {
	server, err := NewServer(&ServerOptions{Addr: listenFreeAddr(t), WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterContextHandler(route.SetState, func(ctx context.Context, conn *ServerConn, seq uint64, request *buffer.Bytes) error {
		_, err := protocol.DecodeSetStateReq(request)
		request.Release()
		if err != nil || trace.SpanContextFromContext(ctx).IsValid() {
			return errors.ErrInvalidMessage
		}
		return conn.Push(protocol.EncodeSetStateRes(seq, codes.OK))
	})
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	conn, err := net.Dial("tcp", server.ListenAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	request := protocol.EncodeHandshakeReq(1, cluster.Node, "legacy", 1)
	_, err = conn.Write(request.Bytes())
	request.Release()
	if err != nil {
		t.Fatal(err)
	}
	header, rt, seq, private, err := readLegacyFrame(conn)
	if err != nil || header != def.DataBit || rt != route.Handshake || seq != 1 || len(private) != def.CodeBytes {
		t.Fatal("new server changed handshake response for legacy client")
	}
	request = protocol.EncodeSetStateReq(2, cluster.Busy)
	_, err = conn.Write(request.Bytes())
	request.Release()
	if err != nil {
		t.Fatal(err)
	}
	header, rt, seq, private, err = readLegacyFrame(conn)
	if err != nil || header != def.DataBit || rt != route.SetState || seq != 2 || len(private) != def.CodeBytes {
		t.Fatal("legacy request was not handled with the previous framing")
	}
}

func TestPropagationSnapshotPreservesSharedPayloadAndSessionDowngrade(t *testing.T) {
	ctx := propagationContext(t)
	payload := buffer.NewBytes([]byte("shared business bytes"))
	frame := protocol.EncodeDeliverReq(0, 100, 200, payload)
	defer frame.Release()
	original := append([]byte(nil), frame.Bytes()...)
	plain, err := snapshotPropagation(context.Background(), frame)
	if err != nil || plain != frame {
		t.Fatal("an untraced frame did not retain the legacy fast path")
	}
	queued, err := snapshotPropagation(ctx, frame)
	if err != nil {
		t.Fatal(err)
	}
	extended := bytes.Join(appendPropagation(nil, queued, true), nil)
	if extended[def.SizeBytes]&def.PropagationMetadataBit == 0 || len(extended) <= len(original) {
		t.Fatal("capable session omitted propagation metadata")
	}
	if !bytes.Equal(frame.Bytes(), original) || !bytes.Equal(payload.Bytes(), []byte("shared business bytes")) {
		t.Fatal("propagation changed shared frame/payload bytes")
	}
	// The exact same queued message may be written after reconnecting to an
	// older peer. Session capability, not enqueue-time capability, controls wire.
	legacy := bytes.Join(appendPropagation(nil, queued, false), nil)
	if !bytes.Equal(legacy, original) {
		t.Fatal("queued metadata leaked into a legacy connection after downgrade")
	}
}

func TestReaderRejectsMalformedPropagationWithoutReturningOwnedBuffer(t *testing.T) {
	for _, testCase := range []struct {
		name string
		data []byte
	}{
		{name: "missing length", data: []byte{1}},
		{name: "zero length", data: []byte{0, 0}},
		{name: "truncated metadata", data: []byte{0, 20, '{'}},
		{name: "invalid JSON", data: []byte{0, 1, '{'}},
		{name: "wrong value type", data: append([]byte{0, 17}, []byte(`{"traceparent":1}`)...)},
		{name: "too large", data: []byte{0x20, 0x01}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client, server := newTCPPair(t)
			frame := dataFrame(route.Deliver, 3, testCase.data)
			frame[def.SizeBytes] |= def.PropagationMetadataBit
			if _, err := server.Write(frame); err != nil {
				t.Fatal(err)
			}
			_, _, _, owned, err := newReader(client).read()
			if !errors.Is(err, errors.ErrInvalidMessage) || owned != nil {
				t.Fatalf("invalid metadata escaped reader ownership: %v %v", owned, err)
			}
		})
	}
}

func TestReaderOwnsMetadataStringsAfterInputReuse(t *testing.T) {
	ctx := propagationContext(t)
	frame := protocol.EncodeSetStateReq(2, cluster.Work)
	defer frame.Release()
	queued, err := snapshotPropagation(ctx, frame)
	if err != nil {
		t.Fatal(err)
	}
	wire := bytes.Join(appendPropagation(nil, queued, true), nil)
	client, server := newTCPPair(t)
	if _, err := server.Write(wire); err != nil {
		t.Fatal(err)
	}
	reader := newReader(client)
	_, _, _, incoming, err := reader.read()
	if err != nil {
		t.Fatal(err)
	}
	metadataSize := int(binary.BigEndian.Uint16(wire[def.MinFrameSize:]))
	metadata := incoming.Prepend(def.B16 + metadataSize)
	for i := range metadata {
		metadata[i] = 'x'
	}
	incoming.Release()
	restored := reader.context()
	if trace.SpanContextFromContext(restored).TraceID() != trace.SpanContextFromContext(ctx).TraceID() || baggage.FromContext(restored).Member("test-key").Value() != "preserved" {
		t.Fatal("context retained aliases into the pooled input buffer")
	}
}

func TestPropagationRejectsOversizedCarrierBeforeQueueing(t *testing.T) {
	ctx := propagationContext(t)
	member, err := baggage.NewMember("huge", string(bytes.Repeat([]byte{'x'}, maxPropagationBytes-16)))
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)
	frame := protocol.EncodeSetStateReq(2, cluster.Work)
	defer frame.Release()
	if _, err := snapshotPropagation(ctx, frame); !errors.Is(err, errors.ErrMessageTooLarge) {
		t.Fatalf("oversized carrier was queued: %v", err)
	}
}
