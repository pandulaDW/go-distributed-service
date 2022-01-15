package server

import (
	"context"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	tlsConfig "github.com/pandulaDW/go-distributed-service/internal/config"
	"github.com/pandulaDW/go-distributed-service/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, rootClient, nobodyClient api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"consume past log boundary fail":                     testConsumePastBoundary,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"produce bulk records":                               testProduceBulkRecords,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(config *Config)) (rootClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0") // 0 will automatically assign a free port
	require.NoError(t, err)

	// Configure server's TLS config
	serverTLSConfig, err := tlsConfig.SetupTLSConfig(tlsConfig.TLSConfig{
		CertFile:      tlsConfig.ServerCertFile,
		KeyFile:       tlsConfig.ServerKeyFile,
		CAFile:        tlsConfig.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{CommitLog: clog}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	// newClient creation closure
	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		// Configure client’s TLS credentials to use the CA as the client’s Root CA
		// The CA it will use to verify the server as well.
		clientTlsConfig, err := tlsConfig.SetupTLSConfig(tlsConfig.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   tlsConfig.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		// Tell the client to use those credentials for its connection
		tlsCreds := credentials.NewTLS(clientTlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		newClient := api.NewLogClient(conn)
		return conn, newClient, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(tlsConfig.RootClientCertFile, tlsConfig.RootClientKeyFile)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(tlsConfig.NobodyClientCertFile, tlsConfig.NobodyClientKeyFile)

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		_ = rootConn.Close()   // close root client connection
		_ = nobodyConn.Close() // close nobody client connection
		_ = l.Close()          // close listener
		_ = clog.Remove()      // close and remove the log
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()
	produce, err := client.Produce(
		ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}},
	)
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: uint64(0)})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{Offset: uint64(i), Value: record.Value})
		}
	}
}

func testProduceBulkRecords(t *testing.T, client, _ api.LogClient, _ *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	stream, err := client.ProduceBulkRecords(ctx)
	require.NoError(t, err)

	for _, record := range records {
		err = stream.Send(&api.ProduceRequest{Record: record})
		require.NoError(t, err)
	}

	res, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, uint64(len(records)), res.NumRecordsInserted)
}
