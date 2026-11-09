package host

import (
	"context"
	"crypto/subtle"
	"errors"
	"log"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tardigradev1 "github.com/rdeusser/tardigrade/go/api/tardigrade/v1"
	"github.com/rdeusser/tardigrade/go/api/tardigrade/v1/tardigradev1connect"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// DefaultReadMaxBytes bounds the request messages the handler reads.
const DefaultReadMaxBytes = 8 << 20

// HandlerOptions configure the HostService handler.
type HandlerOptions struct {
	// Token, when set, requires "Authorization: Bearer <Token>" on every procedure; /healthz stays public.
	Token string
	// ReadMaxBytes bounds request messages; zero uses DefaultReadMaxBytes.
	ReadMaxBytes int
	// Sender identifies API callers in message metadata; the zero value uses {external, api}.
	Sender tardigrade.Sender
}

// Handler serves HostService over the Connect, gRPC, and gRPC-Web protocols, plus GET /healthz. gRPC clients need HTTP/2; serve them with TLS or with http.Server.Protocols allowing unencrypted HTTP/2.
func (h *Host) Handler(opts HandlerOptions) http.Handler {
	if opts.ReadMaxBytes == 0 {
		opts.ReadMaxBytes = DefaultReadMaxBytes
	}
	if opts.Sender == (tardigrade.Sender{}) {
		opts.Sender = tardigrade.Sender{Kind: "external", ID: "api"}
	}
	route, service := tardigradev1connect.NewHostServiceHandler(&service{host: h, sender: opts.Sender},
		connect.WithInterceptors(errorCodes{}), connect.WithReadMaxBytes(opts.ReadMaxBytes))
	mux := http.NewServeMux()
	mux.Handle(route, authorize(opts.Token, service))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

func authorize(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	errs := connect.NewErrorWriter()
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			_ = errs.Write(w, r, connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid bearer token")))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// errorCodes maps host and runtime errors to Connect codes.
type errorCodes struct{}

func (errorCodes) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		return res, code(err)
	}
}

func (errorCodes) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (errorCodes) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		return code(next(ctx, conn))
	}
}

func code(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*connect.Error](err); ok {
		return err
	}
	_, invalid := errors.AsType[*tardigrade.InvalidInputError](err)
	_, stopped := errors.AsType[*tardigrade.StoppedError](err)
	switch {
	case invalid:
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrUnknownActor), errors.Is(err, ErrUnknownThread), errors.Is(err, tardigrade.ErrUnknownMethod), errors.Is(err, tardigrade.ErrUnknownCall):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, tardigrade.ErrMessageConflict):
		return connect.NewError(connect.CodeAborted, err)
	case stopped, errors.Is(err, tardigrade.ErrNotCancellable):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, tardigrade.ErrClosed):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	}
	log.Printf("tardigrade: request failed: %v", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error; inspect the host logs"))
}

type service struct {
	host   *Host
	sender tardigrade.Sender
}

var _ tardigradev1connect.HostServiceHandler = (*service)(nil)

func invalid(message string) error {
	return &tardigrade.InvalidInputError{Err: errors.New(message)}
}

func coordinate(ref *tardigradev1.ThreadRef) tardigrade.ThreadCoordinate {
	return tardigrade.ThreadCoordinate{Actor: ref.GetActor(), Instance: ref.GetInstance(), Thread: ref.GetThread()}
}

func threadRef(address tardigrade.ThreadCoordinate) *tardigradev1.ThreadRef {
	return &tardigradev1.ThreadRef{Actor: address.Actor, Instance: address.Instance, Thread: address.Thread}
}

func (s *service) thread(ref *tardigradev1.ThreadRef) (*tardigrade.Runtime, error) {
	if ref == nil {
		return nil, invalid("thread is required")
	}
	return s.host.Thread(coordinate(ref))
}

func (s *service) ListActors(context.Context, *connect.Request[tardigradev1.ListActorsRequest]) (*connect.Response[tardigradev1.ListActorsResponse], error) {
	res := &tardigradev1.ListActorsResponse{}
	for _, actor := range s.host.Actors() {
		out := &tardigradev1.Actor{Name: actor.Name()}
		for _, m := range actor.Methods() {
			method := &tardigradev1.Method{Name: m.Name, Cancellable: m.Cancellable, InputSchema: &structpb.Struct{}, OutputSchema: &structpb.Struct{}}
			if err := errors.Join(method.InputSchema.UnmarshalJSON(m.InputSchema), method.OutputSchema.UnmarshalJSON(m.OutputSchema)); err != nil {
				return nil, err
			}
			out.Methods = append(out.Methods, method)
		}
		res.Actors = append(res.Actors, out)
	}
	return connect.NewResponse(res), nil
}

func thread(info tardigrade.ThreadInfo) *tardigradev1.Thread {
	out := &tardigradev1.Thread{Ref: threadRef(info.Address), Depth: int32(info.Depth), CreateTime: timestamppb.New(time.UnixMilli(info.CreatedAt)), Active: info.Active}
	if info.Parent != nil {
		out.Parent = threadRef(*info.Parent)
	}
	return out
}

func (s *service) CreateThread(_ context.Context, req *connect.Request[tardigradev1.CreateThreadRequest]) (*connect.Response[tardigradev1.CreateThreadResponse], error) {
	var parent *tardigrade.ThreadCoordinate
	if req.Msg.Parent != nil {
		parent = new(coordinate(req.Msg.Parent))
	}
	address, err := s.host.CreateThread(req.Msg.Actor, req.Msg.Instance, req.Msg.Name, parent)
	if err != nil {
		return nil, err
	}
	info, err := s.host.opts.Store.Thread(address)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&tardigradev1.CreateThreadResponse{Thread: thread(*info)}), nil
}

func (s *service) ListThreads(_ context.Context, req *connect.Request[tardigradev1.ListThreadsRequest]) (*connect.Response[tardigradev1.ListThreadsResponse], error) {
	threads, err := s.host.Threads(req.Msg.Actor, req.Msg.Instance)
	if err != nil {
		return nil, err
	}
	res := &tardigradev1.ListThreadsResponse{}
	for _, info := range threads {
		res.Threads = append(res.Threads, thread(info))
	}
	return connect.NewResponse(res), nil
}

var statuses = map[tardigrade.CallStatus]tardigradev1.CallStatus{
	tardigrade.StatusPending:   tardigradev1.CallStatus_CALL_STATUS_PENDING,
	tardigrade.StatusCompleted: tardigradev1.CallStatus_CALL_STATUS_COMPLETED,
	tardigrade.StatusFailed:    tardigradev1.CallStatus_CALL_STATUS_FAILED,
	tardigrade.StatusCancelled: tardigradev1.CallStatus_CALL_STATUS_CANCELLED,
}

func call(rt *tardigrade.Runtime, id string) (*tardigradev1.Call, error) {
	state, err := rt.Call(id)
	if err != nil {
		return nil, err
	}
	out := &tardigradev1.Call{Thread: threadRef(rt.Address()), Id: id, Method: state.Method, Status: statuses[state.Status]}
	switch state.Status {
	case tardigrade.StatusCompleted:
		output := &structpb.Value{}
		if err := output.UnmarshalJSON(state.Output); err != nil {
			return nil, err
		}
		out.Result = &tardigradev1.Call_Output{Output: output}
	case tardigrade.StatusFailed:
		out.Result = &tardigradev1.Call_Error{Error: state.Error}
	case tardigrade.StatusCancelled:
		out.Result = &tardigradev1.Call_CancelReason{CancelReason: state.Reason}
	}
	return out, nil
}

func (s *service) StartCall(_ context.Context, req *connect.Request[tardigradev1.StartCallRequest]) (*connect.Response[tardigradev1.StartCallResponse], error) {
	if req.Msg.CallId == "" {
		return nil, invalid("call_id is required")
	}
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return nil, err
	}
	input := []byte("{}")
	if req.Msg.Input != nil {
		if input, err = req.Msg.Input.MarshalJSON(); err != nil {
			return nil, &tardigrade.InvalidInputError{Err: err}
		}
	}
	if err := rt.Invoke(req.Msg.Method, req.Msg.CallId, s.sender, input); err != nil {
		return nil, err
	}
	out, err := call(rt, req.Msg.CallId)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&tardigradev1.StartCallResponse{Call: out}), nil
}

func (s *service) GetCall(_ context.Context, req *connect.Request[tardigradev1.GetCallRequest]) (*connect.Response[tardigradev1.GetCallResponse], error) {
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return nil, err
	}
	out, err := call(rt, req.Msg.CallId)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&tardigradev1.GetCallResponse{Call: out}), nil
}

func (s *service) WatchCall(ctx context.Context, req *connect.Request[tardigradev1.WatchCallRequest], stream *connect.ServerStream[tardigradev1.WatchCallResponse]) error {
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return err
	}
	var last *tardigradev1.Call
	for {
		changed := rt.Changed()
		out, err := call(rt, req.Msg.CallId)
		if err != nil {
			return err
		}
		if !proto.Equal(out, last) {
			if err := stream.Send(&tardigradev1.WatchCallResponse{Call: out}); err != nil {
				return err
			}
			last = out
		}
		if out.Status != tardigradev1.CallStatus_CALL_STATUS_PENDING {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *service) CancelCall(_ context.Context, req *connect.Request[tardigradev1.CancelCallRequest]) (*connect.Response[tardigradev1.CancelCallResponse], error) {
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return nil, err
	}
	if err := rt.CancelCall(req.Msg.CallId, req.Msg.Reason, s.sender); err != nil {
		return nil, err
	}
	out, err := call(rt, req.Msg.CallId)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&tardigradev1.CancelCallResponse{Call: out}), nil
}

// records returns up to limit records after the one-based position after; a limit of zero returns all of them.
func records(rt *tardigrade.Runtime, after int64, limit int32) ([]*tardigradev1.Record, error) {
	if after < 0 || limit < 0 {
		return nil, invalid("after and limit must not be negative")
	}
	end := rt.Length()
	if limit > 0 {
		end = min(end, after+int64(limit))
	}
	if after >= end {
		return nil, nil
	}
	read, err := rt.Records(after, end)
	if err != nil {
		return nil, err
	}
	out := make([]*tardigradev1.Record, len(read))
	for i, r := range read {
		data, err := tardigrade.EncodeEvent(r.Event)
		if err != nil {
			return nil, err
		}
		event := &structpb.Struct{}
		if err := event.UnmarshalJSON(data); err != nil {
			return nil, err
		}
		out[i] = &tardigradev1.Record{Seq: after + int64(i) + 1, Type: r.Event.EventType(), Event: event, RecordTime: timestamppb.New(time.UnixMilli(r.RecordedAt))}
		if r.Message != nil {
			out[i].MessageId = r.Message.ID
		}
	}
	return out, nil
}

func (s *service) ListRecords(_ context.Context, req *connect.Request[tardigradev1.ListRecordsRequest]) (*connect.Response[tardigradev1.ListRecordsResponse], error) {
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return nil, err
	}
	out, err := records(rt, req.Msg.After, req.Msg.Limit)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&tardigradev1.ListRecordsResponse{Records: out}), nil
}

func (s *service) WatchRecords(ctx context.Context, req *connect.Request[tardigradev1.WatchRecordsRequest], stream *connect.ServerStream[tardigradev1.WatchRecordsResponse]) error {
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return err
	}
	after := req.Msg.After
	for {
		changed := rt.Changed()
		batch, err := records(rt, after, 0)
		if err != nil {
			return err
		}
		for _, record := range batch {
			if err := stream.Send(&tardigradev1.WatchRecordsResponse{Record: record}); err != nil {
				return err
			}
			after = record.Seq
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *service) WatchUpdates(ctx context.Context, req *connect.Request[tardigradev1.WatchUpdatesRequest], stream *connect.ServerStream[tardigradev1.WatchUpdatesResponse]) error {
	rt, err := s.thread(req.Msg.Thread)
	if err != nil {
		return err
	}
	updates, stop := s.host.Subscribe(rt.Address())
	defer stop()
	for {
		select {
		case update := <-updates:
			payload := &structpb.Value{}
			if err := payload.UnmarshalJSON(update.Payload); err != nil {
				return err
			}
			out := &tardigradev1.Update{
				Thread:    threadRef(update.Address),
				Effect:    &tardigradev1.EffectRef{Seq: update.Ref.Seq, Atom: update.Ref.Atom, Act: update.Ref.Act},
				AttemptId: update.AttemptID, Sequence: int32(update.Sequence), Payload: payload,
			}
			if err := stream.Send(&tardigradev1.WatchUpdatesResponse{Update: out}); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}
