package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	file, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Fatal("os.Remove error")
		}
	}(file.Name())

	config := Config{}
	config.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(file, config)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, file.Name(), idx.Name())

	entries := []struct {
		Offset   uint32
		Position uint64
	}{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Offset, want.Position)
		require.NoError(t, err)

		_, position, err := idx.Read(int64(want.Offset))
		require.NoError(t, err)
		require.Equal(t, want.Position, position)
	}

	// 既存のエントリを超えて読み出す場合、インデックスはエラーを返す
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// インデックスは、既存のファイルからその状態を構築する
	file, _ = os.OpenFile(file.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(file, config)
	require.NoError(t, err)
	offset, position, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, entries[1].Position, position)
}
