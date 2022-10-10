package discovery

import (
	"fmt"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"testing"
	"time"
)

func TestMembership(t *testing.T) {
	/// 複数のサーバーを持つクラスタを設定する
	membership, handler := setupMember(t, nil)
	membership, _ = setupMember(t, membership)
	membership, _ = setupMember(t, membership)

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && len(membership[0].Members()) == 3 && len(handler.leaves) == 0
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, membership[2].Leave())

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && len(membership[0].Members()) == 3 && membership[0].Members()[2].Status == serf.StatusLeft && len(handler.leaves) == 1
	}, 3*time.Second, 250*time.Millisecond)
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	/// 1個の空きのポート番号を含む[]intを返す
	ports := dynaport.Get(1)
	address := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": address,
	}
	config := Config{
		NodeName:    fmt.Sprintf("%d", id),
		BindAddress: address,
		Tags:        tags,
	}
	handler := &handler{}
	if len(members) == 0 {
		handler.joins = make(chan map[string]string, 3)
		handler.leaves = make(chan string, 3)
	} else {
		config.StartJoinAddress = []string{
			members[0].BindAddress,
		}
	}
	membership, err := New(handler, config)
	require.NoError(t, err)
	members = append(members, membership)
	return members, handler
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (handler *handler) Join(id, address string) error {
	if handler.joins != nil {
		handler.joins <- map[string]string{
			"id":      id,
			"address": address,
		}
	}
	return nil
}

func (handler *handler) Leave(id string) error {
	if handler.leaves != nil {
		handler.leaves <- id
	}
	return nil
}
