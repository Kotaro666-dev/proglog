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

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			log.Fatal(err)
		}
	}(dir)

	want := &api.Record{Value: []byte("Hello World")}
	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = totalWidth * 3

	segment, err := newSegment(dir, 16, config)
	require.NoError(t, err)
	require.Equal(t, uint64(16), segment.nextOffset)
	require.False(t, segment.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		offset, err := segment.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, offset)

		got, err := segment.Read(offset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = segment.Append(want)
	require.Equal(t, io.EOF, err)

	// インデックスが最大
	require.True(t, segment.IsMaxed())
	require.NoError(t, segment.Close())

	p, _ := proto.Marshal(want)
	config.Segment.MaxStoreBytes = uint64(len(p)+lenWidth) * 4
	config.Segment.MaxIndexBytes = 1024

	// 既存のセグメントを再構築する
	segment, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	// ストアが最大
	require.True(t, segment.IsMaxed())

	require.NoError(t, segment.Remove())

	segment, err = newSegment(dir, 16, config)
	require.NoError(t, err)
	require.False(t, segment.IsMaxed())
	require.NoError(t, segment.Close())
}
