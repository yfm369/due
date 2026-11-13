package node

import (
	"context"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

// messageContext preserves the application's context factory and imports only
// propagated telemetry from the transport. A connection ending must not cancel
// an already accepted asynchronous business request.
func (n *Node) messageContext(parents ...context.Context) context.Context {
	ctx := context.Background()
	if n.opts.ctxFunc != nil {
		ctx = n.opts.ctxFunc()
	}
	if len(parents) == 0 || parents[0] == nil {
		return ctx
	}
	parent := parents[0]
	if span := trace.SpanFromContext(parent); span.SpanContext().IsValid() {
		ctx = trace.ContextWithSpan(ctx, span)
	}
	if incoming := baggage.FromContext(parent); incoming.Len() > 0 {
		ctx = baggage.ContextWithBaggage(ctx, incoming)
	}
	return ctx
}
