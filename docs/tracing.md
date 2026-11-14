# Cluster tracing context

This fork is based on upstream Due v2.6.7. Its built-in DRPC cluster transport
automatically carries the configured OpenTelemetry text-map context across
node messages and gateway operations. Business message schemas do not need
trace fields or payload wrappers.

## Setup

The framework uses the OpenTelemetry API and leaves SDK providers and exporters
to the application. Configure providers, sampling and propagation at startup:

```go
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{},
    propagation.Baggage{},
))
```

With the default no-op propagator, messages retain their original wire format.
The framework does not create operation spans or decide which requests are
sampled; application middleware and business code keep that responsibility.

## Calls and handlers

`Proxy.Deliver(ctx, args)` and other context-aware cluster operations snapshot
the current propagator's carrier when enqueuing the message. `Context.Deliver`,
`Reply` and `Response` use the request's underlying standard context.

When a middleware or handler creates a span, install the derived context so
subsequent native operations use the active span rather than its incoming parent:

```go
func handle(ctx node.Context) {
    active, span := tracer.Start(ctx.Context(), "game.command")
    ctx.SetContext(active)
    defer span.End()

    // Deliver injects the active context without adding fields to Data.
    _ = ctx.Deliver(&cluster.DeliverArgs{
        NID: "owner-node",
        Message: &cluster.Message{Route: commandRoute, Data: command},
    })
}
```

`SetContext` must receive a non-nil context derived from `Context()` when request
values or cancellation need to be retained. Call it while owning the current
dispatch stage, before transferring ownership with `Next` or `Task`. Do not
cache a request context in a shared actor or connection.

Incoming Deliver and gateway Trigger requests retain the configured
`WithContextFunc` context's values and cancellation while adding the incoming
span and baggage. Actor dispatch, clones, tasks, replies to a local actor and
node-to-node replies preserve the active telemetry context. Closing a transport
connection does not cancel business work already accepted into a node queue.

`node.Context` now includes `SetContext(context.Context)`. Custom Context
implementations, including test doubles, must implement this method. Existing
route handlers that only consume Context do not need signature changes.

## Wire compatibility and limits

Both peers advertise support through an unused bit in the existing handshake
header. Handshake bodies remain unchanged. A separate data-header bit indicates
a bounded, length-prefixed carrier inserted before the original private frame.
The receiver removes it before the original route decoder sees the body.

The writer checks the current connection session's negotiated capability. A
queued message reconnecting to an unmodified v2.6.7 peer uses the original frame
bytes, including unchanged routing, sequence, IDs and business payload.
Compatibility tests cover modified and unmodified upstream v2.6.7 layouts in
both directions. They do not promise compatibility with older releases that had
different upstream protocols.

Tracing continues across a hop when both peers support this extension. A mixed
v2.6.7 deployment continues business communication while legacy hops omit the
tracing carrier. Upgrade all communicating Gate/Node instances to obtain full
propagation.

The encoded carrier is limited to 8 KiB, including its JSON representation.
Oversized carriers return `errors.ErrMessageTooLarge`; malformed metadata is
rejected before business dispatch. Request-owned strings are detached from
pooled receive buffers. Neither the carrier nor another sender's shared payload
is mutated while sending or receiving a frame.

This integration covers built-in cluster communication. Mesh gRPC/RPCX plugins,
event-bus providers and external HTTP calls are distinct transports and keep
their respective instrumentation contracts.

## Verification

Regression tests exercise native TCP Call/Push, node Deliver, gate Trigger,
active-handler Reply/Response, Actor/Task ownership, factory context semantics,
sequential-message isolation, legacy peers, malformed metadata, buffer ownership
and session downgrade. Run the root module tests and applicable race checks:

```sh
go test ./...
go test -race ./cluster/node ./internal/transporter/...
go vet ./...
go build ./...
```

All default vet analyzers remain enabled. The fixed-width binary reader method
previously named `ReadRune(order)` is now `ReadRune32(order)`, avoiding a collision
with the standard `ReadRune` method signature. Consumers using that older name
must update their calls; the byte order, four-byte encoding and EOF behavior are
unchanged.

The application migration also tests an actual WebSocket gateway, two game
nodes and a room actor with no business-payload tracing fields set.
