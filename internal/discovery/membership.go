package discovery

import (
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"net"
)

// Membership / ディスカバリとクラスタメンバーシップを提供する
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	membership := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := membership.setupSerf(); err != nil {
		return nil, err
	}
	return membership, nil
}

type Config struct {
	NodeName         string
	BindAddress      string
	Tags             map[string]string
	StartJoinAddress []string
}

func (membership *Membership) setupSerf() (err error) {
	address, err := net.ResolveTCPAddr("tcp", membership.BindAddress)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = address.IP.String()
	config.MemberlistConfig.BindPort = address.Port

	membership.events = make(chan serf.Event)
	config.EventCh = membership.events
	config.Tags = membership.Tags
	config.NodeName = membership.Config.NodeName
	membership.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go membership.eventHandler()
	if membership.StartJoinAddress != nil {
		_, err = membership.serf.Join(membership.StartJoinAddress, true)
		if err != nil {
			return err
		}
	}
	return nil
}

type Handler interface {
	Join(name, address string) error
	Leave(name string) error
}

func (membership *Membership) eventHandler() {
	for event := range membership.events {
		switch event.EventType() {
		case serf.EventMemberJoin:
			for _, member := range event.(serf.MemberEvent).Members {
				if membership.isLocal(member) {
					continue
				}
				membership.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range event.(serf.MemberEvent).Members {
				if membership.isLocal(member) {
					return
				}
				membership.handleLeave(member)
			}
		}
	}
}

func (membership *Membership) handleJoin(member serf.Member) {
	if err := membership.handler.Join(
		member.Name,
		member.Tags["rpc_addr"]); err != nil {
		membership.logError(err, "failed to join", member)
	}
}

func (membership *Membership) handleLeave(member serf.Member) {
	if err := membership.handler.Leave(member.Name); err != nil {
		membership.logError(err, "failed to leave", member)
	}
}

/// 指定されたSerfメンバーがローカルメンバーか判定数r
func (membership *Membership) isLocal(member serf.Member) bool {
	return membership.serf.LocalMember().Name == member.Name
}

func (membership *Membership) Members() []serf.Member {
	return membership.serf.Members()
}

func (membership *Membership) Leave() error {
	return membership.serf.Leave()
}

func (membership *Membership) logError(err error, message string, member serf.Member) {
	membership.logger.Error(
		message, zap.Error(err), zap.String("name", member.Name), zap.String("rpc_addr", member.Tags["rpc_addr"]))
}
