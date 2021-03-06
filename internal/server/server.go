package server

import (
	"context"
	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcCtxTags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"io"
	"strings"
	"time"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// NewGRPCServer instantiate the log service, create a gRPC server, and register the
// service to that server
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	// set logger options
	logger := zap.L().Named("server")
	zapOpts := []grpcZap.Option{
		grpcZap.WithDurationField(func(duration time.Duration) zapcore.Field {
			return zap.Int64("grpc.time_ns", duration.Nanoseconds())
		}),
	}

	// add tracing for only production requests and for half of other requests randomly
	halfSampler := trace.ProbabilitySampler(0.5)
	trace.ApplyConfig(trace.Config{DefaultSampler: func(p trace.SamplingParameters) trace.SamplingDecision {
		if strings.Contains(p.Name, "Produce") {
			return trace.SamplingDecision{Sample: true}
		}
		return halfSampler(p)
	}})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.StreamInterceptor(
		grpcMiddleware.ChainStreamServer(
			grpcCtxTags.StreamServerInterceptor(),
			grpcZap.StreamServerInterceptor(logger, zapOpts...),
			grpcAuth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpcMiddleware.ChainUnaryServer(
		grpcCtxTags.UnaryServerInterceptor(),
		grpcZap.UnaryServerInterceptor(logger, zapOpts...),
		grpcAuth.UnaryServerInterceptor(authenticate),
	)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// newgrpcServer creates a new server instance
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

// Produce implements the Produce handler
func (srv *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	err := srv.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction)
	if err != nil {
		return nil, err
	}
	off, err := srv.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: off}, nil
}

// Consume implements the Consume-handler
func (srv *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	err := srv.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction)
	if err != nil {
		return nil, err
	}
	record, err := srv.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream implements a bidirectional streaming RPC so the client can stream data into the server???s
// log and the server can tell the client whether each request succeeded
func (srv *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := srv.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream implements a server-side streaming RPC so the client can tell the server where in the log to
// read records, and then the server will stream every record that follows???even records that aren???t in the log yet!
//
// When the server reaches the end of the log, the server will wait until someone appends a record to the log
// and then continue streaming records to the client
func (srv *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := srv.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// ProduceBulkRecords implements a streaming RPC for client to bulk insert records to reduce the number
// of connections maintained when inserting a large number of records at once.
func (srv *grpcServer) ProduceBulkRecords(stream api.Log_ProduceBulkRecordsServer) error {
	insertCount := uint64(0)

loop:
	for {
		select {
		case <-stream.Context().Done():
			break loop
		default:
			req, err := stream.Recv()
			if err == io.EOF {
				break loop
			}
			if err != nil {
				return err
			}
			_, err = srv.CommitLog.Append(req.Record)
			if err != nil {
				return err
			}
			insertCount++
		}
	}

	return stream.SendAndClose(&api.ProduceBulkResponse{NumRecordsInserted: insertCount})
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

// authenticate is an interceptor that reads the subject out of the client???s cert and writes it to the RPC???s context.
func authenticate(ctx context.Context) (context.Context, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}

	if peerInfo.AuthInfo == nil {
		return ctx, status.New(codes.Unauthenticated, "no transport security being used").Err()
	}

	tlsInfo := peerInfo.AuthInfo.(credentials.TLSInfo)
	subjectName := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subjectName)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
