package log

import (
	"github.com/stretchr/testify/require"
	api "github/Kotaro666-dev/prolog/api/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"appendRecord and read a record succeeds": testAppendRead,
		"offset out of range error":               testOutOfRangeError,
		"init with existing segments":             testInitExisting,
		"reader":                                  testReader,
		"truncate":                                testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer func(path string) {
				err := os.RemoveAll(path)
				if err != nil {
					log.Fatal(err)
				}
			}(dir)

			config := Config{}
			// 一つのセグメントに最大2つしか書き込めない。
			// 3つのレコードを書き込むと、二つのセグメントが作成される
			config.Segment.MaxStoreBytes = 32
			newLog, err := NewLog(dir, config)
			require.NoError(t, err)

			fn(t, newLog)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	appendRecord := &api.Record{
		Value: []byte("Hello World"),
	}
	offset, err := log.Append(appendRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	read, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, appendRecord.Value, read.Value)
	require.NoError(t, log.Close())
}

func testOutOfRangeError(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
	require.NoError(t, log.Close())
}

func testInitExisting(t *testing.T, log *Log) {
	appendRecord := &api.Record{
		Value: []byte("Hello World"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(appendRecord)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	offset, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	offset, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)

	log2, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	offset, err = log2.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	offset, err = log2.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), offset)
	require.NoError(t, log2.Close())
}

func testReader(t *testing.T, log *Log) {
	appendRecord := &api.Record{
		Value: []byte("Hello World"),
	}
	offset, err := log.Append(appendRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(bytes[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, appendRecord.Value, read.Value)
	require.NoError(t, log.Close())
}

func testTruncate(t *testing.T, log *Log) {
	appendRecord := &api.Record{
		Value: []byte("Hello World"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(appendRecord)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
	require.NoError(t, log.Close())
}
