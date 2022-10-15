package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"github/Kotaro666-dev/prolog/internal/auth"
	"github/Kotaro666-dev/prolog/internal/discovery"
	"github/Kotaro666-dev/prolog/internal/log"
	"github/Kotaro666-dev/prolog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"net"
	"sync"
	"time"
)

type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig  *tls.Config
	PeerTLSConfig    *tls.Config
	DataDir          string
	BindAddress      string
	RPCPort          int
	NodeName         string
	StartJoinAddress []string
	ACLModelFile     string
	ACLPolicyFile    string
	Bootstrap        bool
}

func (c Config) RPCAddress() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddress)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	agent := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		agent.setupLogger,
		agent.setupMux,
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go func() {
		err := agent.serve()
		if err != nil {
			return
		}
	}()
	return agent, nil
}

func (agent *Agent) setupMux() error {
	rpcAddress := fmt.Sprintf(":%d", agent.Config.RPCPort)
	listener, err := net.Listen("tcp", rpcAddress)
	if err != nil {
		return err
	}
	agent.mux = cmux.New(listener)
	return nil
}

func (agent *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (agent *Agent) setupLog() error {
	raftListener := agent.mux.Match(func(reader io.Reader) bool {
		buffer := make([]byte, 1)
		if _, err := reader.Read(buffer); err != nil {
			return false
		}
		return bytes.Equal(buffer, []byte{byte(log.RaftRPC)})
	})

	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftListener,
		agent.Config.ServerTLSConfig,
		agent.Config.PeerTLSConfig)
	logConfig.Raft.LocalID = raft.ServerID(agent.Config.NodeName)
	logConfig.Raft.Bootstrap = agent.Config.Bootstrap

	var err error
	agent.log, err = log.NewDistributedLog(
		agent.Config.DataDir,
		logConfig)
	if err != nil {
		return err
	}
	if agent.Config.Bootstrap {
		err = agent.log.WaitForLeader(3 * time.Second)
	}
	return err
}

func (agent *Agent) setupServer() error {
	authorizer := auth.New(
		agent.Config.ACLModelFile,
		agent.Config.ACLPolicyFile)
	serverConfig := &server.Config{CommitLog: agent.log, Authorizer: authorizer}
	var options []grpc.ServerOption
	if agent.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(agent.Config.ServerTLSConfig)
		options = append(options, grpc.Creds(creds))
	}
	var err error
	agent.server, err = server.NewGrpcServer(serverConfig, options...)
	if err != nil {
		return err
	}
	grpcListener := agent.mux.Match(cmux.Any())
	go func() {
		if err := agent.server.Serve(grpcListener); err != nil {
			_ = agent.Shutdown()
		}
	}()
	return err
}

func (agent *Agent) setupMembership() error {
	rpcAddress, err := agent.Config.RPCAddress()
	if err != nil {
		return err
	}
	agent.membership, err = discovery.New(agent.log, discovery.Config{
		NodeName:    agent.Config.NodeName,
		BindAddress: agent.Config.BindAddress,
		Tags: map[string]string{
			"rpc_addr": rpcAddress,
		},
		StartJoinAddress: agent.Config.StartJoinAddress,
	})
	return err
}

func (agent *Agent) Shutdown() error {
	agent.shutdownLock.Lock()
	defer agent.shutdownLock.Unlock()
	if agent.shutdown {
		return nil
	}
	agent.shutdown = true
	close(agent.shutdowns)

	shutdown := []func() error{
		/// 他のサーバはこのサーバがクラスタから離脱したことを認識し、このサーバはディスカばりのイベントを受信しなくなる
		agent.membership.Leave,
		/// 新たなコネクションを受け付けないようにして、保留中のRPCがすべて終了するまで待ちます
		func() error {
			agent.server.GracefulStop()
			return nil
		},
		agent.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (agent *Agent) serve() error {
	if err := agent.mux.Serve(); err != nil {
		_ = agent.Shutdown()
		return err
	}
	return nil
}
