package node_test

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/node"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type traceProvider struct {
	node.Provider

	delivered chan context.Context
	triggered chan context.Context
}

func (p *traceProvider) Deliver(ctx context.Context, _, _ string, _, _ int64, message buffer.Buffer) error {
	message.Release()
	p.delivered <- ctx
	return nil
}

func (p *traceProvider) Trigger(ctx context.Context, _ string, _, _ int64, _ cluster.Event) error {
	p.triggered <- ctx
	return nil
}

func traceParent(t *testing.T, sampled bool) (context.Context, trace.SpanContext) {
	t.Helper()
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	state, err := trace.ParseTraceState("vendor=node-transport-test")
	if err != nil {
		t.Fatal(err)
	}
	flags := trace.TraceFlags(0)
	if sampled {
		flags = trace.FlagsSampled
	}
	span := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: flags, TraceState: state,
	})
	member, err := baggage.NewMember("test-origin", "node-transport")
	if err != nil {
		t.Fatal(err)
	}
	items, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(context.Background(), items)
	return trace.ContextWithSpanContext(ctx, span), span
}

func assertRemoteTrace(t *testing.T, received <-chan context.Context, parent trace.SpanContext) {
	t.Helper()
	select {
	case ctx := <-received:
		span := trace.SpanContextFromContext(ctx)
		if span.TraceID() != parent.TraceID() || span.SpanID() != parent.SpanID() || !span.IsRemote() {
			t.Errorf("node transport lost the remote parent: got=%v want=%v", span, parent)
		}
		if span.TraceFlags() != parent.TraceFlags() || span.TraceState().String() != parent.TraceState().String() {
			t.Error("node transport changed sampling flags or tracestate")
		}
		if baggage.FromContext(ctx).Member("test-origin").Value() != "node-transport" {
			t.Error("node transport dropped baggage")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node transport did not deliver the request")
	}
}

func assertNoPreviousTrace(t *testing.T, received <-chan context.Context) {
	t.Helper()
	select {
	case ctx := <-received:
		if trace.SpanContextFromContext(ctx).IsValid() || baggage.FromContext(ctx).Len() != 0 {
			t.Fatal("request on a reused connection inherited a previous trace")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("untraced request did not arrive")
	}
}

func TestNodeTransportDeliverPropagatesEachRequestContext(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	p := &traceProvider{delivered: make(chan context.Context, 1)}
	server := newNodeServer(t, p)
	client := buildNodeClient(t, server.ListenAddr(), clientOptionsForKind("trace-source", cluster.Node))
	t.Cleanup(func() { _ = client.Close() })
	for _, sampled := range []bool{true, false} {
		ctx, parent := traceParent(t, sampled)
		if err := client.Deliver(ctx, 10, 20, buffer.NewNocopyBuffer([]byte("unchanged payload"))); err != nil {
			t.Fatal(err)
		}
		assertRemoteTrace(t, p.delivered, parent)
		if err := client.Deliver(context.Background(), 10, 20, buffer.NewNocopyBuffer([]byte("untraced payload"))); err != nil {
			t.Fatal(err)
		}
		assertNoPreviousTrace(t, p.delivered)
	}
}

func TestNodeTransportTriggerPropagatesEachEventContext(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	p := &traceProvider{triggered: make(chan context.Context, 1)}
	server := newNodeServer(t, p)
	client := buildNodeClient(t, server.ListenAddr(), clientOptionsForKind("trace-gate", cluster.Gate))
	t.Cleanup(func() { _ = client.Close() })
	ctx, parent := traceParent(t, false)
	if err := client.Trigger(ctx, cluster.Connect, 10, 20); err != nil {
		t.Fatal(err)
	}
	assertRemoteTrace(t, p.triggered, parent)
	if err := client.Trigger(context.Background(), cluster.Disconnect, 10, 20); err != nil {
		t.Fatal(err)
	}
	assertNoPreviousTrace(t, p.triggered)
}
