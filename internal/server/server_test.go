package server

import (
	"context"
	"flag"
	"github.com/stretchr/testify/require"
	api "github/Kotaro666-dev/prolog/api/v1"
	"github/Kotaro666-dev/prolog/internal/auth"
	"github/Kotaro666-dev/prolog/internal/config"
	"github/Kotaro666-dev/prolog/internal/log"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"testing"
	"time"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging")

/// テスト実行時の前処理
/// デバッグ出力の有効化など、そのファイル内の全てのテストに適用される設定を行う場所を提供する
func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func()) {
	t.Helper()

	/// 指定している0番ポートは、自動的に空きポートを割り当ててくれる
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	/// 96Pより追記
	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		options := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(listener.Addr().String(), options...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, options
	}

	/// 書き込みと読み出しを許可されたrootというスーパーユーザクライアント
	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile)

	/// 何も許可されていないnobodyクライアント
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile)

	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: listener.Addr().String(),
			Server:        true,
		})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	/// P114より追記
	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
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

	return rootClient, nobodyClient, cfg, func() {
		err := rootConn.Close()
		if err != nil {
			return
		}
		err = nobodyConn.Close()
		if err != nil {
			return
		}
		server.Stop()
		err = listener.Close()
		if err != nil {
			return
		}
		if telemetryExporter != nil {
			/// データをディスクにフラッシュするのに十分な時間を与えるためのスリープ
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
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
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
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

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
