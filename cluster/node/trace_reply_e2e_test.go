package node

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/errors"
	nodetransport "github.com/dobyte/due/v2/internal/transporter/node"
	"github.com/dobyte/due/v2/registry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type traceRegistry struct {
	registry.Registry

	nodes []*registry.ServiceInstance
	done  chan struct{}
}

func (r *traceRegistry) Watch(context.Context, string) (registry.Watcher, error) {
	return &traceWatcher{nodes: append([]*registry.ServiceInstance(nil), r.nodes...), done: r.done}, nil
}

type traceWatcher struct {
	nodes []*registry.ServiceInstance
	done  <-chan struct{}
	first bool
}

func (w *traceWatcher) Next() ([]*registry.ServiceInstance, error) {
	if !w.first {
		w.first = true
		return w.nodes, nil
	}
	<-w.done
	return nil, errors.ErrWatcherStopped
}

func (*traceWatcher) Stop() error { return nil }

func startTraceNodeServer(t *testing.T, n *Node) *nodetransport.Server {
	t.Helper()
	server, err := nodetransport.NewServer(&provider{node: n},
		&nodetransport.ServerOptions{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	return server
}

func TestNodeReplyContinuesActiveHandlerTraceAcrossTCP(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "reply"
		if async {
			name = "task-response"
		}
		t.Run(name, func(t *testing.T) {
			previous := otel.GetTextMapPropagator()
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{}, propagation.Baggage{}))
			t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
			r := &traceRegistry{done: make(chan struct{})}
			source := newTestNode(t, WithID("trace-reply-source"), WithRegistry(r))
			target := newTestNode(t, WithID("trace-reply-target"), WithRegistry(r))
			upstream, parent := routedTraceParent(t)
			activeID, err := trace.SpanIDFromHex("3333333333333333")
			if err != nil {
				t.Fatal(err)
			}
			active := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: parent.TraceID(), SpanID: activeID, TraceFlags: parent.TraceFlags(),
				TraceState: parent.TraceState(),
			})
			received := make(chan context.Context, 1)
			response := make(chan context.Context, 1)
			replyErrors := make(chan error, 1)
			source.router.AddRouteHandler(103, func(ctx Context) {
				var payload string
				if err := ctx.Parse(&payload); err != nil || payload != "reply" ||
					ctx.Seq() != 7 || ctx.UID() != 42 || ctx.NID() != target.opts.id {
					t.Errorf("reply payload or origin changed: payload=%q, err=%v", payload, err)
				}
				response <- ctx.Context()
			})
			target.router.AddRouteHandler(103, func(ctx Context) {
				received <- ctx.Context()
				ctx.SetContext(trace.ContextWithSpanContext(ctx.Context(), active))
				if async {
					ctx.Task(func(task Context) { replyErrors <- task.Response("reply") })
				} else {
					replyErrors <- ctx.Reply(&cluster.Message{Route: 103, Seq: 7, Data: "reply"})
				}
			})
			setWorking(source)
			setWorking(target)
			sourceServer, targetServer := startTraceNodeServer(t, source), startTraceNodeServer(t, target)
			r.nodes = []*registry.ServiceInstance{
				{ID: source.opts.id, Name: "node", Kind: "node", Alias: source.opts.name,
					State: "work", Endpoint: sourceServer.Endpoint().String()},
				{ID: target.opts.id, Name: "node", Kind: "node", Alias: target.opts.name,
					State: "work", Endpoint: targetServer.Endpoint().String()},
			}
			t.Cleanup(func() {
				close(r.done)
				source.cancel()
				target.cancel()
				_ = source.proxy.nodeLinker.Close()
				_ = target.proxy.nodeLinker.Close()
			})
			source.proxy.nodeLinker.WatchClusterInstance()
			target.proxy.nodeLinker.WatchClusterInstance()
			deadline := time.Now().Add(2 * time.Second)
			for !source.Proxy().HasNode(target.opts.id) || !target.Proxy().HasNode(source.opts.id) {
				if time.Now().After(deadline) {
					t.Fatal("node discovery did not complete")
				}
				time.Sleep(time.Millisecond)
			}
			if err := source.Proxy().Deliver(upstream, &cluster.DeliverArgs{
				NID: target.opts.id, UID: 42,
				Message: &cluster.Message{Route: 103, Seq: 7, Data: "request"},
			}); err != nil {
				t.Fatal(err)
			}
			target.router.handle(waitActorReplyRegression(t, target.router.receive()))
			incoming := waitActorReplyRegression(t, received)
			if got := trace.SpanContextFromContext(incoming); got.SpanID() != parent.SpanID() ||
				got.TraceID() != parent.TraceID() || !got.IsRemote() {
				t.Error("target did not receive the original remote parent")
			}
			if err := waitActorReplyRegression(t, replyErrors); err != nil {
				t.Fatal(err)
			}
			source.router.handle(waitActorReplyRegression(t, source.router.receive()))
			returned := waitActorReplyRegression(t, response)
			span := trace.SpanContextFromContext(returned)
			if span.TraceID() != parent.TraceID() || span.SpanID() != activeID || !span.IsRemote() ||
				span.TraceFlags() != parent.TraceFlags() || span.TraceState().String() != parent.TraceState().String() {
				t.Fatalf("reply did not continue the active handler trace: %v", span)
			}
			if baggage.FromContext(returned).Member("test-origin").Value() != "provider" {
				t.Fatal("reply dropped the incoming baggage")
			}
		})
	}
}

func TestActorReplyKeepsActiveTraceAndFactoryContext(t *testing.T) {
	type factoryValue struct{}
	base := context.WithValue(context.Background(), factoryValue{}, "receiver factory")
	n := newTestNode(t, WithContextFunc(func() context.Context { return base }))
	_, parent := routedTraceParent(t)
	var source *Actor
	n.router.AddRouteHandler(104, func(ctx Context) {
		ctx.SetContext(trace.ContextWithSpanContext(ctx.Context(), parent))
		if err := ctx.Reply(&cluster.Message{Route: 104, Seq: ctx.Seq(), Data: "reply"}); err != nil {
			t.Errorf("actor reply failed: %v", err)
		}
	})
	setWorking(n)
	returned := make(chan context.Context, 1)
	source = newActorReplyRegressionActor(t, n, "trace-reply-actor", map[int32]RouteHandler{
		104: func(ctx Context) { returned <- ctx.Context() },
	})
	if err := source.Push(42, &cluster.Message{Route: 104, Seq: 7, Data: "request"}); err != nil {
		t.Fatal(err)
	}
	n.router.handle(waitActorReplyRegression(t, n.router.receive()))
	ctx := waitActorReplyRegression(t, returned)
	if trace.SpanContextFromContext(ctx).TraceID() != parent.TraceID() ||
		ctx.Value(factoryValue{}) != "receiver factory" {
		t.Fatal("local actor reply discarded active trace or receiver factory values")
	}
}
