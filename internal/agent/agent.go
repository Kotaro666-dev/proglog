package agent

import (
	"crypto/tls"
	"fmt"
	api "github/Kotaro666-dev/prolog/api/v1"
	"github/Kotaro666-dev/prolog/internal/auth"
	"github/Kotaro666-dev/prolog/internal/discovery"
	"github/Kotaro666-dev/prolog/internal/log"
	"github/Kotaro666-dev/prolog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync"
)

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

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
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return agent, nil
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
	var err error
	agent.log, err = log.NewLog(
		agent.Config.DataDir,
		log.Config{})
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
	rpcAddress, err := agent.Config.RPCAddress()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", rpcAddress)
	if err != nil {
		return err
	}
	go func() {
		if err := agent.server.Serve(listener); err != nil {
			_ = agent.shutdown
		}
	}()
	return err
}

func (agent *Agent) setupMembership() error {
	rpcAddress, err := agent.Config.RPCAddress()
	if err != nil {
		return err
	}
	var options []grpc.DialOption
	if agent.Config.PeerTLSConfig != nil {
		options = append(options, grpc.WithTransportCredentials(
			credentials.NewTLS(agent.Config.PeerTLSConfig)))
	}
	clientConn, err := grpc.Dial(rpcAddress, options...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(clientConn)
	agent.replicator = &log.Replicator{
		DialOptions: options,
		LocalServer: client,
	}
	agent.membership, err = discovery.New(agent.replicator, discovery.Config{
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
		/// レプリケータを閉じて、複製を続けないようにする
		agent.replicator.Close,
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
