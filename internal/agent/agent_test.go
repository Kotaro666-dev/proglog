package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github/Kotaro666-dev/prolog/api/v1"
	"github/Kotaro666-dev/prolog/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"os"
	"testing"
	"time"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        true,
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        false,
	})
	require.NoError(t, err)

	var agents []*Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddress := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddress []string
		if i != 0 {
			startJoinAddress = append(
				startJoinAddress,
				agents[0].Config.BindAddress)
		}

		newAgent, err := New(Config{
			ServerTLSConfig:  serverTLSConfig,
			PeerTLSConfig:    peerTLSConfig,
			DataDir:          dataDir,
			BindAddress:      bindAddress,
			RPCPort:          rpcPort,
			NodeName:         fmt.Sprintf("%d", i),
			StartJoinAddress: startJoinAddress,
			ACLModelFile:     config.ACLModelFile,
			ACLPolicyFile:    config.ACLPolicyFile,
		})
		require.NoError(t, err)

		agents = append(agents, newAgent)
	}

	defer func() {
		for _, currentAgent := range agents {
			err := currentAgent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(currentAgent.Config.DataDir))
		}
	}()
	/// ノードが互いを発見する時間を確保するため、テストを数秒間スリープさせています。
	time.Sleep(3 * time.Second)

	/// 一つのノードに対して書き込み、そのノードから読み出せることをテスト
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{Value: []byte("foo")}})
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Consume(context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset})
	require.NoError(t, err)

	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	/// 別のノードがレコードを複製したかどうかをテスト
	// レプリケーションが完了するまで待つ
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))
}

func client(t *testing.T, agent *Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	options := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddress, err := agent.Config.RPCAddress()
	require.NoError(t, err)

	clientConn, err := grpc.Dial(rpcAddress, options...)
	require.NoError(t, err)

	client := api.NewLogClient(clientConn)
	return client
}
