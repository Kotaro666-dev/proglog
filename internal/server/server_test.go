package server

import (
	"context"
	"github.com/stretchr/testify/require"
	api "github/Kotaro666-dev/prolog/api/v1"
	"github/Kotaro666-dev/prolog/internal/config"
	"github/Kotaro666-dev/prolog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func()) {
	t.Helper()

	/// 指定している0番ポートは、自動的に空きポートを割り当ててくれる
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	/// 89Pより追記
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile: config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(
		listener.Addr().String(),
		grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)
	client = api.NewLogClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: listener.Addr().String(),
		})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGrpcServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		err := server.Serve(listener)
		if err != nil {
			return
		}
	}()

	return client, cfg, func() {
		err := cc.Close()
		if err != nil {
			return
		}
		server.Stop()
		err = listener.Close()
		if err != nil {
			return
		}
		err = clog.Remove()
		if err != nil {
			return
		}
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("Hello World"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: want,
	},
	)
	require.NoError(t, err)
	want.Offset = produce.Offset

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

/// クライアントがログの教会を超えて読み出そうとした場合に発生するエラーテスト
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("Hello World"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("Consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrorOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			response, err := stream.Recv()
			require.NoError(t, err)
			if response.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d", response.Offset, offset,
				)
			}
		}

	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			response, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, response.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}
