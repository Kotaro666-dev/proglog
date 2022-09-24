package log

import (
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
)

var (
	write = []byte("Hello World!")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	file, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Fatal("os.Remove error")
		}
	}(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, store)
	testRead(t, store)
	testReedAt(t, store)

	store, err = newStore(file)
	require.NoError(t, err)
	testRead(t, store)
}

func testAppend(t *testing.T, store *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		bytes, position, err := store.Append(write)
		require.NoError(t, err)
		require.Equal(t, position+bytes, width+1)
	}
}

func testRead(t *testing.T, store *store) {
	t.Helper()
	var position uint64

	for i := uint64(1); i < 4; i++ {
		bytes, err := store.Read(position)
		require.NoError(t, err)
		require.Equal(t, write, bytes)
		position += width
	}

}

func testReedAt(t *testing.T, store *store) {
	t.Helper()

	for i, offset := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		bytes, err := store.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, bytes)
		offset += int64(bytes)

		size := enc.Uint64(b)
		b = make([]byte, size)
		bytes, err = store.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), bytes)
		offset += int64(bytes)
	}
}

func TestStoreClose(t *testing.T) {
	file, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Fatal("os.remove error")
		}
	}(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)
	require.NoError(t, err)

	_, _, err = store.Append(write)
	require.NoError(t, err)

	file, beforeSize, err := openFile(file.Name())
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(file.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
