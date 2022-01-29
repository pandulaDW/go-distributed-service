package log

import (
	"context"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
)

// Replicator connects to other servers with the gRPC client.
//
// clientOptions field is used to configure the client to authenticate with the servers.
//
// the server field is a map of server addresses to a channel, which the replicator
// uses to stop replicating from a server when the server fails or leaves the cluster
//
// The replicator calls the produce function to save a copy of the messages it consumes
// from the other servers
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

// init is a helper function to initialize few values
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// replicate creates a client connection that consumes all logs on the server
// using a streaming client request.
//
// the client also runs a loop which consumes the logs from the discovered server in a stream
// and then produces to the local server to save a copy
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	clientConn, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial ", addr)
		return
	}
	defer func(clientConn *grpc.ClientConn) {
		_ = clientConn.Close()
	}(clientConn)

	client := api.NewLogClient(clientConn)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)

	go func() {
		for {
			received, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- received.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave method handles the server leaving the cluster by removing the server from
// the list of servers to replicate and closes the server’s associated channel
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// Join method adds the given server address to the list of servers to
// replicate and kicks off a goroutine to run the actual replication logic.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already replicating, so skip
		return nil
	}

	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil
}

// Close closes the replicator service, so it doesn't replicate new servers that join the
// cluster, and it stops replicating existing servers by causing the replicate() goroutines to return
func (r *Replicator) Close() error {
	r.mu.Lock()
	r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	r.closed = true
	close(r.close)
	return nil
}

// logError logs error message for replicator
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(msg, zap.String("addr", addr), zap.Error(err))
}
