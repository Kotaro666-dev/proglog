package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github/Kotaro666-dev/prolog/api/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

type DistributedLog struct {
	config  Config
	log     *Log
	raftLog *logStore
	raft    *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	log := &DistributedLog{
		config: config,
	}
	if err := log.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := log.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return log, nil
}

func (dl *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	dl.log, err = NewLog(logDir, dl.config)
	if err != nil {
		return err
	}
	return nil
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	var err error

	/// FSM: finite-state machine 有限ステートマシン
	fsm := &fsm{log: dl.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := dl.config
	logConfig.Segment.InitialOffset = 1
	dl.raftLog, err = newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// 安定ストア: KVS
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "eaft"), retain, nil)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		dl.config.Raft.StreamLayer, maxPool, timeout, nil)

	config := raft.DefaultConfig()
	/// このサーバの一意なIDで、唯一設定しなければならないもの
	config.LocalID = dl.config.Raft.LocalID
	if dl.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = dl.config.Raft.HeartbeatTimeout
	}
	if dl.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = dl.config.Raft.ElectionTimeout
	}
	if dl.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = dl.config.Raft.LeaderLeaseTimeout
	}
	if dl.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = dl.config.Raft.CommitTimeout
	}
	dl.raft, err = raft.NewRaft(
		config, fsm, dl.raftLog, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		dl.raftLog, stableStore, snapshotStore)
	if err != nil {
		return err
	}
	if dl.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{Servers: []raft.Server{
			{ID: config.LocalID, Address: transport.LocalAddr()}}}
		err = dl.raft.BootstrapCluster(config).Error()
	}
	return nil
}

func (dl *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := dl.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

func (dl *DistributedLog) apply(requestType RequestType, req proto.Message) (interface{}, error) {
	var buffer bytes.Buffer
	_, err := buffer.Write([]byte{byte(requestType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(requestType)
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	///  レコードを複製してリーダーのログにレコードを追加する処理を実行している
	future := dl.raft.Apply(buffer.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

/// 強い一貫性が必要な場合、読み出しは書き込みに対して最新であるべきのため、Raftを経由する必要がある
/// その場合、読み出し効率が悪くなるとともに、時間がかかる
func (dl *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return dl.log.Read(offset)
}

func (dl *DistributedLog) Join(id, address string) error {
	configFuture := dl.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddress := raft.ServerAddress(address)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddress {
			// サーバはすでに参加している
			return nil
		}
		// 既存のサーバを取り除く
		removeFuture := dl.raft.RemoveServer(serverID, 0, 0)
		if err := removeFuture.Error(); err != nil {
			return err
		}
	}
	addFuture := dl.raft.AddVoter(serverID, serverAddress, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (dl *DistributedLog) Leave(id string) error {
	removeFuture := dl.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (dl *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if serverAddress, _ := dl.raft.LeaderWithID(); serverAddress != "" {
				return nil
			}
		}
	}
}

func (dl *DistributedLog) Close() error {
	f := dl.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	if err := dl.raftLog.Log.Close(); err != nil {
		return err
	}
	return dl.log.Close()
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

func (r RequestType) ProtoReflect() protoreflect.Message {
	//TODO implement me
	panic("implement me")
}

const (
	AppendRequestType RequestType = 0
)

func (f *fsm) Apply(record *raft.Log) interface{} {
	buffer := record.Data
	requestType := RequestType(buffer[0])
	switch requestType {
	case AppendRequestType:
		return f.applyAppend(buffer[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var request api.ProduceRequest
	err := proto.Unmarshal(b, &request)
	if err != nil {
		return err
	}
	offset, err := f.log.Append(request.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	reader := f.log.Reader()
	return &snapshot{reader: reader}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {
	//TODO implement me
}

func (f *fsm) Restore(readCloser io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buffer bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(readCloser, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buffer, readCloser, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buffer.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buffer.Reset()
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

func newLogStore(dir string, config Config) (*logStore, error) {
	log, err := NewLog(dir, config)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func (s StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

func (s StreamLayer) Close() error {
	return s.ln.Close()
}

func (s StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

const RaftRPC = 1

// Dial Raftクラスタ内の他のサーバに出ていくコネクションを作るもの
func (s *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}
	// Raft RPC であることを特定する
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, nil
}

func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}
