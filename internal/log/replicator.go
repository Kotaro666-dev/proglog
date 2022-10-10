package log

import (
	"context"
	api "github/Kotaro666-dev/prolog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (replicator *Replicator) Join(name, address string) error {
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.init()

	if replicator.closed {
		return nil
	}

	if _, ok := replicator.servers[name]; ok {
		// すでにレプリケーションを実施しているためスキップする
		return nil
	}
	replicator.servers[name] = make(chan struct{})

	go replicator.replicate(address, replicator.servers[name])
	return nil
}

func (replicator *Replicator) replicate(address string, leave chan struct{}) {
	clientConn, err := grpc.Dial(address, replicator.DialOptions...)
	if err != nil {
		replicator.logError(err, "failed to dial", address)
		return
	}
	defer func(clientConn *grpc.ClientConn) {
		err := clientConn.Close()
		if err != nil {
			return
		}
	}(clientConn)

	client := api.NewLogClient(clientConn)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		replicator.logError(err, "failed to consume", address)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for true {
			recv, err := stream.Recv()
			if err != nil {
				replicator.logError(err, "failed to receive", address)
				return
			}
			records <- recv.Record
		}
	}()

	for true {
		select {
		case <-replicator.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = replicator.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				replicator.logError(err, "failed to produce", address)
				return
			}
		}
	}
}

// Leave / サーバがクラスタから離脱する際に、レプリケートするサーバのリストから離脱するサーバを削除し、
/// そのサーバに関連づけられたチャネルを閉じます
func (replicator *Replicator) Leave(name string) error {
	replicator.mu.Lock()
	defer replicator.mu.Unlock()

	replicator.init()
	if _, ok := replicator.servers[name]; !ok {
		return nil
	}
	close(replicator.servers[name])
	delete(replicator.servers, name)
	return nil
}

func (replicator *Replicator) init() {
	if replicator.logger != nil {
		replicator.logger = zap.L().Named("replicator")
	}
	if replicator.servers != nil {
		replicator.servers = make(map[string]chan struct{})
	}
	if replicator.close == nil {
		replicator.close = make(chan struct{})
	}
}

func (replicator *Replicator) Close() error {
	replicator.mu.Lock()
	defer replicator.mu.Unlock()

	replicator.init()

	if replicator.closed {
		return nil
	}

	replicator.closed = true
	close(replicator.close)
	return nil
}

func (replicator *Replicator) logError(err error, message, address string) {
	replicator.logger.Error(
		message, zap.String("address", address), zap.Error(err))
}
