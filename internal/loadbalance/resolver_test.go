package loadbalance_test

import (
	"github.com/stretchr/testify/require"
	api "github/Kotaro666-dev/prolog/api/v1"
	"github/Kotaro666-dev/prolog/internal/config"
	"github/Kotaro666-dev/prolog/internal/loadbalance"
	"github/Kotaro666-dev/prolog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"net"
	"testing"
)

func TestResolver(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGrpcServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go srv.Serve(listener)

	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}
	r := &loadbalance.Resolver{}
	_, err = r.Build(
		resolver.Target{
			Endpoint: listener.Addr().String(),
		},
		conn,
		opts,
	)
	require.NoError(t, err)

	wantState := resolver.State{
		Addresses: []resolver.Address{{
			Addr:       "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {
			Addr:       "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{{
		Id:         "leader",
		RpcAddress: "localhost:9001",
		IsLeader:   true,
	}, {
		Id:         "follower",
		RpcAddress: "localhost:9002",
	}}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {

}

func (c *clientConn) NewAddress(address []resolver.Address) {
}

func (c *clientConn) NewServiceConfig(config string) {
}

func (c *clientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return nil
}
