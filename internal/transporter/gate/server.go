package gate

import (
	"context"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/internal/codes"
	"github.com/dobyte/due/v2/internal/transporter/internal/drpc"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
	"github.com/dobyte/due/v2/internal/transporter/internal/route"
)

type Server struct {
	*drpc.Server
	provider Provider
}

type ServerOptions = drpc.ServerOptions

func NewServer(provider Provider, opts *ServerOptions) (*Server, error) {
	serv, err := drpc.NewServer(opts)
	if err != nil {
		return nil, err
	}

	s := &Server{Server: serv, provider: provider}
	s.init()

	return s, nil
}

func (s *Server) init() {
	s.RegisterContextHandler(route.Bind, s.bindContext)
	s.RegisterContextHandler(route.Unbind, s.unbindContext)
	s.RegisterContextHandler(route.GetIP, s.getIPContext)
	s.RegisterContextHandler(route.Stat, s.statContext)
	s.RegisterContextHandler(route.IsOnline, s.isOnlineContext)
	s.RegisterContextHandler(route.Disconnect, s.disconnectContext)
	s.RegisterContextHandler(route.Push, s.pushContext)
	s.RegisterContextHandler(route.Multicast, s.multicastContext)
	s.RegisterContextHandler(route.Broadcast, s.broadcastContext)
	s.RegisterContextHandler(route.Publish, s.publishContext)
	s.RegisterContextHandler(route.Subscribe, s.subscribeContext)
	s.RegisterContextHandler(route.Unsubscribe, s.unsubscribeContext)
	s.RegisterHandler(route.GetState, s.getState)
	s.RegisterHandler(route.SetState, s.setState)
}

// bind binds the user to the connection.
func (s *Server) bind(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.bindContext(context.Background(), conn, seq, req)
}

func (s *Server) bindContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	cid, uid, err := protocol.DecodeBindReq(req)

	req.Release()

	if err != nil {
		return err
	}

	err = s.provider.Bind(ctx, cid, uid)

	if seq == 0 {
		return err
	}

	return conn.Push(protocol.EncodeBindRes(seq, codes.ErrorToCode(err)))
}

// unbind unbinds the user from its connection.
func (s *Server) unbind(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.unbindContext(context.Background(), conn, seq, req)
}

func (s *Server) unbindContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	uid, err := protocol.DecodeUnbindReq(req)

	req.Release()

	if err != nil {
		return err
	}

	err = s.provider.Unbind(ctx, uid)

	if seq == 0 {
		return err
	}

	return conn.Push(protocol.EncodeUnbindRes(seq, codes.ErrorToCode(err)))
}

// getIP returns the client IP address.
func (s *Server) getIP(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.getIPContext(context.Background(), conn, seq, req)
}

func (s *Server) getIPContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, target, err := protocol.DecodeGetIPReq(req)

	req.Release()

	if err != nil {
		return err
	}

	ip, err := s.provider.GetIP(ctx, kind, target)

	if seq == 0 {
		return err
	}

	return conn.Push(protocol.EncodeGetIPRes(seq, codes.ErrorToCode(err), ip))
}

// stat returns the total number of online sessions.
func (s *Server) stat(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.statContext(context.Background(), conn, seq, req)
}

func (s *Server) statContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, err := protocol.DecodeStatReq(req)

	req.Release()

	if err != nil {
		return err
	}

	total, err := s.provider.Stat(ctx, kind)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeStatRes(seq, codes.ErrorToCode(err), uint64(total)))
	}
}

// isOnline reports whether the target session is online.
func (s *Server) isOnline(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.isOnlineContext(context.Background(), conn, seq, req)
}

func (s *Server) isOnlineContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, target, err := protocol.DecodeIsOnlineReq(req)

	req.Release()

	if err != nil {
		return err
	}

	isOnline, err := s.provider.IsOnline(ctx, kind, target)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeIsOnlineRes(seq, codes.ErrorToCode(err), isOnline))
	}
}

// disconnect disconnects the target session.
func (s *Server) disconnect(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.disconnectContext(context.Background(), conn, seq, req)
}

func (s *Server) disconnectContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, target, force, err := protocol.DecodeDisconnectReq(req)

	req.Release()

	if err != nil {
		return err
	}

	err = s.provider.Disconnect(ctx, kind, target, force)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeDisconnectRes(seq, codes.ErrorToCode(err)))
	}
}

// push pushes a single message.
//
// Note that buf is not released here; it must be released when the message is sent.
func (s *Server) push(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.pushContext(context.Background(), conn, seq, req)
}

func (s *Server) pushContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, target, disconnect, buf, err := protocol.DecodePushReq(req)
	if err != nil {
		req.Release()
		return err
	}

	err = s.provider.Push(ctx, kind, target, disconnect, buf)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodePushRes(seq, codes.ErrorToCode(err)))
	}
}

// multicast pushes a message to multiple targets.
func (s *Server) multicast(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.multicastContext(context.Background(), conn, seq, req)
}

func (s *Server) multicastContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, targets, disconnect, buf, err := protocol.DecodeMulticastReq(req)
	if err != nil {
		req.Release()
		return err
	}

	total, err := s.provider.Multicast(ctx, kind, targets, disconnect, buf)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeMulticastRes(seq, codes.ErrorToCode(err), uint64(total)))
	}
}

// broadcast pushes a message to all sessions.
func (s *Server) broadcast(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.broadcastContext(context.Background(), conn, seq, req)
}

func (s *Server) broadcastContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, disconnect, buf, err := protocol.DecodeBroadcastReq(req)
	if err != nil {
		req.Release()
		return err
	}

	total, err := s.provider.Broadcast(ctx, kind, disconnect, buf)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeBroadcastRes(seq, codes.ErrorToCode(err), uint64(total)))
	}
}

// publish publishes a channel message.
func (s *Server) publish(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.publishContext(context.Background(), conn, seq, req)
}

func (s *Server) publishContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	channel, disconnect, buf, err := protocol.DecodePublishReq(req)
	if err != nil {
		req.Release()
		return err
	}

	total, err := s.provider.Publish(ctx, channel, disconnect, buf)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodePublishRes(seq, codes.ErrorToCode(err), uint64(total)))
	}
}

// subscribe subscribes the targets to a channel.
func (s *Server) subscribe(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.subscribeContext(context.Background(), conn, seq, req)
}

func (s *Server) subscribeContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, targets, channel, err := protocol.DecodeSubscribeReq(req)

	req.Release()

	if err != nil {
		return err
	}

	err = s.provider.Subscribe(ctx, kind, targets, channel)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeSubscribeRes(seq, codes.ErrorToCode(err)))
	}
}

// unsubscribe unsubscribes the targets from a channel.
func (s *Server) unsubscribe(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	return s.unsubscribeContext(context.Background(), conn, seq, req)
}

func (s *Server) unsubscribeContext(ctx context.Context, conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	kind, targets, channel, err := protocol.DecodeUnsubscribeReq(req)

	req.Release()

	if err != nil {
		return err
	}

	err = s.provider.Unsubscribe(ctx, kind, targets, channel)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeUnsubscribeRes(seq, codes.ErrorToCode(err)))
	}
}

// getState returns the state.
func (s *Server) getState(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	err := protocol.DecodeGetStateReq(req)

	req.Release()

	if err != nil {
		return err
	}

	state, err := s.provider.GetState()

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeGetStateRes(seq, codes.ErrorToCode(err), state))
	}
}

// setState sets the state.
func (s *Server) setState(conn *drpc.ServerConn, seq uint64, req *buffer.Bytes) error {
	state, err := protocol.DecodeSetStateReq(req)

	req.Release()

	if err != nil {
		return err
	}

	err = s.provider.SetState(state)

	if seq == 0 {
		return err
	} else {
		return conn.Push(protocol.EncodeSetStateRes(seq, codes.ErrorToCode(err)))
	}
}
