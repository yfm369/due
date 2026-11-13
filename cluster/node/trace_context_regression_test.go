package node

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

type traceStage struct {
	name string
	ctx  context.Context
}

func routedTraceParent(t *testing.T) (context.Context, trace.SpanContext) {
	t.Helper()
	id, err := trace.TraceIDFromHex("fedcba9876543210fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("fedcba9876543210")
	if err != nil {
		t.Fatal(err)
	}
	state, err := trace.ParseTraceState("vendor=router-test")
	if err != nil {
		t.Fatal(err)
	}
	member, err := baggage.NewMember("test-origin", "provider")
	if err != nil {
		t.Fatal(err)
	}
	items, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: id, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: state, Remote: true,
	})
	base := baggage.ContextWithBaggage(context.Background(), items)
	return trace.ContextWithRemoteSpanContext(base, parent), parent
}

func TestIncomingTraceSurvivesRouterActorAndTask(t *testing.T) {
	type factoryValue struct{}
	factory, cancel := context.WithCancel(context.WithValue(context.Background(), factoryValue{}, "local value"))
	defer cancel()
	n := newTestNode(t, WithContextFunc(func() context.Context { return factory }))
	stages := make(chan traceStage, 1)
	completed := make(chan struct{}, 1)
	var actor *Actor
	n.router.AddRouteHandler(101, func(ctx Context) {
		stages <- traceStage{name: "router", ctx: ctx.Context()}
		if err := actor.Next(ctx); err != nil {
			t.Errorf("Actor.Next failed: %v", err)
		}
	})
	n.router.SetPostRouteHandler(func(Context) { completed <- struct{}{} })
	setWorking(n)
	actor = newActorReplyRegressionActor(t, n, "trace-owner", map[int32]RouteHandler{
		101: func(ctx Context) {
			stages <- traceStage{name: "actor", ctx: ctx.Context()}
			ctx.Task(func(task Context) { stages <- traceStage{name: "task", ctx: task.Context()} })
		},
	})
	p := &provider{node: n}
	incoming, parent := routedTraceParent(t)
	for _, traced := range []bool{true, false} {
		ctx := context.Background()
		if traced {
			ctx = incoming
		}
		if err := p.Deliver(ctx, "", "source-node", 10, 20, packTestMessage(t, 101)); err != nil {
			t.Fatal(err)
		}
		request := waitActorReplyRegression(t, n.router.receive())
		n.router.handle(request)
		var received []traceStage
		for range 3 {
			stage := waitActorReplyRegression(t, stages)
			received = append(received, stage)
			if stage.ctx.Value(factoryValue{}) != "local value" {
				t.Errorf("%s discarded the configured local context", stage.name)
			}
			span := trace.SpanContextFromContext(stage.ctx)
			if traced {
				if span.TraceID() != parent.TraceID() || span.SpanID() != parent.SpanID() ||
					!span.IsRemote() || span.TraceFlags() != parent.TraceFlags() ||
					span.TraceState().String() != parent.TraceState().String() {
					t.Errorf("%s dropped incoming trace: %v", stage.name, span)
				}
				if baggage.FromContext(stage.ctx).Member("test-origin").Value() != "provider" {
					t.Errorf("%s discarded propagated baggage", stage.name)
				}
			} else if span.IsValid() || baggage.FromContext(stage.ctx).Len() != 0 {
				t.Errorf("%s leaked context from the preceding request", stage.name)
			}
		}
		cancel()
		for _, stage := range received {
			if stage.ctx.Err() != context.Canceled {
				t.Errorf("%s detached the configured context cancellation", stage.name)
			}
		}
		waitActorReplyRegression(t, completed)
	}
}

func TestIncomingEventTraceSurvivesTriggerQueue(t *testing.T) {
	n := newTestNode(t)
	observed := make(chan trace.SpanContext, 1)
	n.Proxy().AddEventHandler(cluster.Connect, func(ctx Context) { observed <- trace.SpanContextFromContext(ctx.Context()) })
	setWorking(n)
	ctx, parent := routedTraceParent(t)
	if err := (&provider{node: n}).Trigger(ctx, "source-gate", 10, 20, cluster.Connect); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-n.trigger.receive():
		n.trigger.handle(event)
	case <-time.After(time.Second):
		t.Fatal("event was not queued")
	}
	if got := waitActorReplyRegression(t, observed); got.TraceID() != parent.TraceID() || !got.IsRemote() {
		t.Fatalf("event trigger discarded incoming trace: %v", got)
	}
}

func TestSetContextKeepsActiveSpanThroughActorAndTask(t *testing.T) {
	type routeValue struct{}
	n := newTestNode(t)
	stages := make(chan traceStage, 1)
	completed := make(chan struct{}, 1)
	_, incoming := routedTraceParent(t)
	routerSpanID, err := trace.SpanIDFromHex("1111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	actorSpanID, err := trace.SpanIDFromHex("2222222222222222")
	if err != nil {
		t.Fatal(err)
	}
	routerSpan := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: incoming.TraceID(), SpanID: routerSpanID, TraceFlags: trace.FlagsSampled,
	})
	actorSpan := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: incoming.TraceID(), SpanID: actorSpanID, TraceFlags: trace.FlagsSampled,
	})
	var actor *Actor
	n.router.AddRouteHandler(102, func(ctx Context) {
		active := trace.ContextWithSpanContext(ctx.Context(), routerSpan)
		ctx.SetContext(context.WithValue(active, routeValue{}, "active request"))
		stages <- traceStage{name: "router", ctx: ctx.Context()}
		if err := actor.Next(ctx); err != nil {
			t.Errorf("Actor.Next failed: %v", err)
		}
	})
	n.router.SetPostRouteHandler(func(Context) { completed <- struct{}{} })
	setWorking(n)
	actor = newActorReplyRegressionActor(t, n, "active-trace-owner", map[int32]RouteHandler{
		102: func(ctx Context) {
			if trace.SpanContextFromContext(ctx.Context()).SpanID() != routerSpanID {
				t.Error("actor did not inherit the active router span")
			}
			ctx.SetContext(trace.ContextWithSpanContext(ctx.Context(), actorSpan))
			stages <- traceStage{name: "actor", ctx: ctx.Context()}
			ctx.Task(func(task Context) { stages <- traceStage{name: "task", ctx: task.Context()} })
		},
	})
	if err := (&provider{node: n}).Deliver(context.Background(), "", "source-node", 10, 20, packTestMessage(t, 102)); err != nil {
		t.Fatal(err)
	}
	n.router.handle(waitActorReplyRegression(t, n.router.receive()))
	for _, expected := range []trace.SpanID{routerSpanID, actorSpanID, actorSpanID} {
		stage := waitActorReplyRegression(t, stages)
		if trace.SpanContextFromContext(stage.ctx).SpanID() != expected ||
			stage.ctx.Value(routeValue{}) != "active request" {
			t.Errorf("%s lost its explicitly installed active span or values", stage.name)
		}
	}
	waitActorReplyRegression(t, completed)
}
