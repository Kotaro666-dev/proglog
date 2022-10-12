package log

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github/Kotaro666-dev/prolog/api/v1"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	for i := 0; i < nodeCount; i++ {
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen(
			"tcp",
			fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := Config{}
		config.Raft.StreamLayer = NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		// 書籍よりも長めに設定した
		// この時間が短いと、リーダー格の持続時間が短く、リーダーが不在となってしまいがち
		config.Raft.HeartbeatTimeout = 300 * time.Millisecond
		config.Raft.ElectionTimeout = 300 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 300 * time.Millisecond
		config.Raft.CommitTimeout = 50 * time.Millisecond

		if i == 0 {
			// クラスタをブートストラップしてリーダになる
			config.Raft.Bootstrap = true
		}

		l, err := NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			err = logs[0].Join(
				fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		logs = append(logs, l)
	}

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	for _, record := range records {
		offset, err := logs[0].Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(offset)
				if err != nil {
					return false
				}
				record.Offset = offset
				// Raftがリーダのサーバに追加したレコードをフォロワーに複製しているか確認する
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// リーダーがクラスタから離脱したサーバへのレプリケーションを停止し、既存のサーバへのレプリケーションは継続していることをテストする
	err := logs[0].Leave("1")
	require.NoError(t, err)

	// 書籍よりも長めにスリープにした。
	// この時間が短いと、リーダーからフォロワーへのレプリケーションが完了しておらずにテストで失敗する
	time.Sleep(100 * time.Millisecond)

	offset, err := logs[0].Append(&api.Record{Value: []byte("third")})
	require.NoError(t, err)

	// 書籍よりも長めにスリープにした。
	// この時間が短いと、リーダーからフォロワーへのレプリケーションが完了しておらずにテストで失敗する
	time.Sleep(100 * time.Millisecond)

	record, err := logs[1].Read(offset)
	require.IsType(t, api.ErrorOffsetOutOfRange{}, err)
	require.Nil(t, record)

	record, err = logs[2].Read(offset)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, offset, record.Offset)
}
