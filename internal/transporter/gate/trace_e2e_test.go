package gate_test

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/gate"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type gateTraceObservation struct {
	operation string
	ctx       context.Context
}

type gateTraceProvider struct {
	gate.Provider
	seen chan gateTraceObservation
}

func (p *gateTraceProvider) observe(ctx context.Context, operation string) {
	p.seen <- gateTraceObservation{operation: operation, ctx: ctx}
}
func (p *gateTraceProvider) Bind(ctx context.Context, _, _ int64) error {
	p.observe(ctx, "bind")
	return nil
}
func (p *gateTraceProvider) Unbind(ctx context.Context, _ int64) error {
	p.observe(ctx, "unbind")
	return nil
}
func (p *gateTraceProvider) GetIP(ctx context.Context, _ cluster.Session, _ int64) (string, error) {
	p.observe(ctx, "get-ip")
	return "127.0.0.1", nil
}
func (p *gateTraceProvider) Stat(ctx context.Context, _ cluster.Session) (int64, error) {
	p.observe(ctx, "stat")
	return 1, nil
}
func (p *gateTraceProvider) IsOnline(ctx context.Context, _ cluster.Session, _ int64) (bool, error) {
	p.observe(ctx, "is-online")
	return true, nil
}
func (p *gateTraceProvider) Disconnect(ctx context.Context, _ cluster.Session, _ int64, _ bool) error {
	p.observe(ctx, "disconnect")
	return nil
}
func (p *gateTraceProvider) Push(ctx context.Context, _ cluster.Session, _ int64, _ bool, message buffer.Buffer) error {
	message.Release()
	p.observe(ctx, "push")
	return nil
}
func (p *gateTraceProvider) Multicast(ctx context.Context, _ cluster.Session, _ []int64, _ bool, message buffer.Buffer) (int64, error) {
	message.Release()
	p.observe(ctx, "multicast")
	return 1, nil
}
func (p *gateTraceProvider) Broadcast(ctx context.Context, _ cluster.Session, _ bool, message buffer.Buffer) (int64, error) {
	message.Release()
	p.observe(ctx, "broadcast")
	return 1, nil
}
func (p *gateTraceProvider) Publish(ctx context.Context, _ string, _ bool, message buffer.Buffer) (int64, error) {
	message.Release()
	p.observe(ctx, "publish")
	return 1, nil
}
func (p *gateTraceProvider) Subscribe(ctx context.Context, _ cluster.Session, _ []int64, _ string) error {
	p.observe(ctx, "subscribe")
	return nil
}
func (p *gateTraceProvider) Unsubscribe(ctx context.Context, _ cluster.Session, _ []int64, _ string) error {
	p.observe(ctx, "unsubscribe")
	return nil
}

func TestGateTransportPropagatesEveryOperationAndIsolatesNextMessage(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	provider := &gateTraceProvider{seen: make(chan gateTraceObservation, 1)}
	server := startGateServer(t, provider)
	options := newTestClientOptions("gate-propagation-source")
	options.Kind, options.ConnNum = cluster.Node, 1
	client := buildGateClient(t, server.ListenAddr(), options)
	t.Cleanup(func() { _ = client.Close() })
	id, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	state, _ := trace.ParseTraceState("vendor=gate-test")
	member, _ := baggage.NewMember("gate-origin", "native")
	bag, _ := baggage.New(member)
	testCases := []struct {
		operation string
		call      func(context.Context) error
	}{
		{"bind", func(ctx context.Context) error { return client.Bind(ctx, 1, 2) }},
		{"unbind", func(ctx context.Context) error { return client.Unbind(ctx, 2) }},
		{"get-ip", func(ctx context.Context) error { _, err := client.GetIP(ctx, cluster.User, 2); return err }},
		{"stat", func(ctx context.Context) error { _, err := client.Stat(ctx, cluster.User); return err }},
		{"is-online", func(ctx context.Context) error { _, err := client.IsOnline(ctx, cluster.User, 2); return err }},
		{"disconnect", func(ctx context.Context) error { return client.Disconnect(ctx, cluster.User, 2, false) }},
		{"push", func(ctx context.Context) error { return client.Push(ctx, cluster.User, 2, false, msg(), true) }},
		{"push", func(ctx context.Context) error { return client.Push(ctx, cluster.User, 2, false, msg(), false) }},
		{"multicast", func(ctx context.Context) error {
			_, err := client.Multicast(ctx, cluster.User, []int64{2}, false, msg(), true)
			return err
		}},
		{"broadcast", func(ctx context.Context) error {
			_, err := client.Broadcast(ctx, cluster.User, false, msg(), true)
			return err
		}},
		{"publish", func(ctx context.Context) error {
			_, err := client.Publish(ctx, "channel", false, msg(), true)
			return err
		}},
		{"publish", func(ctx context.Context) error {
			_, err := client.Publish(ctx, "channel", false, msg(), false)
			return err
		}},
		{"subscribe", func(ctx context.Context) error { return client.Subscribe(ctx, cluster.User, []int64{2}, "channel") }},
		{"unsubscribe", func(ctx context.Context) error { return client.Unsubscribe(ctx, cluster.User, []int64{2}, "channel") }},
	}
	for _, sampled := range []bool{true, false} {
		flags := trace.TraceFlags(0)
		if sampled {
			flags = trace.FlagsSampled
		}
		parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: spanID, TraceFlags: flags, TraceState: state})
		traced := trace.ContextWithSpanContext(baggage.ContextWithBaggage(context.Background(), bag), parent)
		for _, testCase := range testCases {
			for _, input := range []context.Context{traced, context.Background()} {
				if err := testCase.call(input); err != nil {
					t.Fatalf("%s failed: %v", testCase.operation, err)
				}
				select {
				case observed := <-provider.seen:
					if observed.operation != testCase.operation {
						t.Fatalf("%s reached %s", testCase.operation, observed.operation)
					}
					got := trace.SpanContextFromContext(observed.ctx)
					if input == traced {
						if got.TraceID() != parent.TraceID() || got.SpanID() != parent.SpanID() || !got.IsRemote() || got.TraceFlags() != flags || got.TraceState().String() != state.String() || baggage.FromContext(observed.ctx).Member("gate-origin").Value() != "native" {
							t.Errorf("%s lost propagated context (sampled=%v): %v", testCase.operation, sampled, got)
						}
					} else if got.IsValid() || baggage.FromContext(observed.ctx).Len() != 0 {
						t.Errorf("%s leaked the previous message trace", testCase.operation)
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("%s did not reach gate provider", testCase.operation)
				}
			}
		}
	}
}
