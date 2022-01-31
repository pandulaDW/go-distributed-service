package log

import (
	"context"
	"fmt"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	"github.com/pandulaDW/go-distributed-service/internal/auth"
	tlsConfig "github.com/pandulaDW/go-distributed-service/internal/config"
	"github.com/pandulaDW/go-distributed-service/internal/server"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"
)

func TestReplicator(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, r *Replicator, primaryAddr string){
		"replicates the logs from a primary server":  testReplicatorJoin,
		"replicator leaves the service successfully": testReplicatorLeave,
		"replicator closes the service successfully": testReplicatorClose,
	} {
		t.Run(scenario, func(t *testing.T) {
			r, primaryAddr, tearDownFn := setupTest(t)
			defer tearDownFn()
			fn(t, r, primaryAddr)
		})
	}
}

func setupTest(t *testing.T) (*Replicator, string, func()) {
	t.Helper()
	ctx := context.Background()
	ports := dynaport.Get(2)

	// create the primary server and its tcp listener
	lPrimary, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[0]))
	require.NoError(t, err)
	primary, dirPrimary := setupServer(t)
	go func() {
		_ = primary.Serve(lPrimary)
	}()

	// create a client to call the primary server
	clientOpts := setupClientOpts(t)
	primaryClientConn, err := grpc.Dial(lPrimary.Addr().String(), clientOpts...)
	require.NoError(t, err)
	clientForPrimary := api.NewLogClient(primaryClientConn)

	// create few logs in the primary
	for i := 0; i < 3; i++ {
		_, _ = clientForPrimary.Produce(ctx, &api.ProduceRequest{
			Record: &api.Record{Value: []byte(fmt.Sprintf("hello, world %d", i+1))},
		})
	}

	// create the secondary server and its tcp listener
	lSecondary, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[1]))
	require.NoError(t, err)
	secondary, dirSecondary := setupServer(t)
	go func() {
		_ = secondary.Serve(lSecondary)
	}()

	// create a client for the secondary
	clientOpts = setupClientOpts(t)
	secondaryClientConn, err := grpc.Dial(lSecondary.Addr().String(), clientOpts...)
	require.NoError(t, err)
	clientForSecondary := api.NewLogClient(secondaryClientConn)

	// setup replicator with the secondary client and the primary client dial options
	r := Replicator{}
	r.DialOptions = clientOpts
	r.LocalServer = clientForSecondary

	// teardown function
	teardown := func() {
		primary.Stop()
		secondary.Stop()
		_ = primaryClientConn.Close()
		_ = secondaryClientConn.Close()
		_ = lPrimary.Close()
		_ = lSecondary.Close()
		_ = os.RemoveAll(dirPrimary)
		_ = os.RemoveAll(dirSecondary)
	}

	return &r, lPrimary.Addr().String(), teardown
}

func testReplicatorJoin(t *testing.T, r *Replicator, primaryAddr string) {
	ctx := context.Background()
	err := r.Join("primary", primaryAddr)
	require.NoError(t, err)

	// wait until the replication finishes since the replication process is asynchronous
	time.Sleep(2 * time.Second)

	for i := 0; i < 3; i++ {
		res, err := r.LocalServer.Consume(ctx, &api.ConsumeRequest{Offset: uint64(i)})
		require.NoError(t, err)
		require.Equal(t, res.Record.Value, []byte(fmt.Sprintf("hello, world %d", i+1)))
	}
}

func testReplicatorLeave(t *testing.T, r *Replicator, primaryAddr string) {
	err := r.Join("primary", primaryAddr)
	require.NoError(t, err)

	err = r.Leave("primary")
	require.NoError(t, err)

	require.Empty(t, r.servers)
}

func testReplicatorClose(t *testing.T, r *Replicator, primaryAddr string) {
	err := r.Join("primary", primaryAddr)
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	require.True(t, r.closed)
	_, isOpen := <-r.close
	require.False(t, isOpen)
}

func setupServer(t *testing.T) (*grpc.Server, string) {
	t.Helper()
	// setup grpcServer creds
	serverTlsConfig, err := tlsConfig.SetupTLSConfig(tlsConfig.TLSConfig{
		CertFile:      tlsConfig.ServerCertFile,
		KeyFile:       tlsConfig.ServerKeyFile,
		CAFile:        tlsConfig.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        true,
	})
	require.NoError(t, err)
	tlsCreds := credentials.NewTLS(serverTlsConfig)

	// setup logger and authorizer
	dir, err := ioutil.TempDir("", fmt.Sprintf("replication-%d", rand.Int()))
	require.NoError(t, err)

	cLog, err := NewLog(dir, Config{})
	require.NoError(t, err)
	authorizer := auth.New(tlsConfig.ACLModelFile, tlsConfig.ACLPolicyFile)

	// create the grpcServer
	cfg := server.Config{CommitLog: cLog, Authorizer: authorizer}
	grpcServer, err := server.NewGRPCServer(&cfg, grpc.Creds(tlsCreds))
	require.NoError(t, err)

	return grpcServer, dir
}

func setupClientOpts(t *testing.T) []grpc.DialOption {
	t.Helper()
	// setup client tls config
	clientTlsConfig, err := tlsConfig.SetupTLSConfig(tlsConfig.TLSConfig{
		CertFile:      tlsConfig.RootClientCertFile,
		KeyFile:       tlsConfig.RootClientKeyFile,
		CAFile:        tlsConfig.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        false,
	})
	require.NoError(t, err)
	tlsCreds := credentials.NewTLS(clientTlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	return opts
}
