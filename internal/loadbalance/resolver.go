package loadbalance

import (
	"context"
	"fmt"
	api "github/Kotaro666-dev/prolog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"sync"
)

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

const Name = "proglog"

func (r Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOptions []grpc.DialOption
	if opts.DialCreds != nil {
		dialOptions = append(
			dialOptions, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig: [{"%s": {}}]"}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOptions...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, err
}

func (r Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Builder = (*Resolver)(nil)

var _ resolver.Resolver = (*Resolver)(nil)

func (r Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	// クラスタを取得s知恵、ClientConnの状態をこうしんする
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server", zap.Error(err))
		return
	}
	var address []resolver.Address
	for _, server := range res.Servers {
		address = append(address, resolver.Address{
			Addr: server.RpcAddress,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader),
		})
	}

	r.clientConn.UpdateState(resolver.State{
		Addresses:     address,
		ServiceConfig: r.serviceConfig,
	})
}

func (r Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn", zap.Error(err))
	}
}
