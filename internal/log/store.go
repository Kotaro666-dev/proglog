package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(file.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

func (store *store) Append(data []byte) (n uint64, position uint64, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	position = store.size

	err = binary.Write(store.buf, enc, uint64(len(data)))
	if err != nil {
		return 0, 0, err
	}

	bytes, err := store.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}

	bytes += lenWidth
	store.size += uint64(bytes)
	return uint64(bytes), position, nil
}

func (store *store) Read(position uint64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	/// バッファがまだディスクにフラッシュされていないレコードを読み出そうとしている場合を考慮している
	err := store.buf.Flush()
	if err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	_, err = store.File.ReadAt(size, int64(position))
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, enc.Uint64(size))
	_, err = store.File.ReadAt(bytes, int64(position+lenWidth))
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (store *store) ReadAt(position []byte, offset int64) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	err := store.buf.Flush()
	if err != nil {
		return 0, err
	}
	return store.File.ReadAt(position, offset)
}

func (store *store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	err := store.buf.Flush()
	if err != nil {
		return err
	}
	return store.File.Close()
}
