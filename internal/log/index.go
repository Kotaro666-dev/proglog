package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	totalWidth           = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(file *os.File, config Config) (*index, error) {
	idx := &index{
		file: file,
	}
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	err = os.Truncate(file.Name(), int64(config.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (index *index) Close() error {
	if err := index.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := index.file.Sync(); err != nil {
		return err
	}

	if err := index.file.Truncate(int64(index.size)); err != nil {
		return err
	}
	return index.file.Close()
}

func (index *index) Read(input int64) (output uint32, position uint64, err error) {
	if index.size == 0 {
		return 0, 0, io.EOF
	}

	if input == -1 {
		output = uint32((index.size / totalWidth) - 1)
	} else {
		output = uint32(input)
	}

	position = uint64(output) * totalWidth
	if index.size < position+totalWidth {
		return 0, 0, io.EOF
	}
	output = enc.Uint32(index.mmap[position : position+offsetWidth])
	position = enc.Uint64(index.mmap[position+offsetWidth : position+totalWidth])
	return output, position, nil
}

func (index *index) Write(offset uint32, position uint64) error {
	if index.isMaxed() {
		return io.EOF
	}

	enc.PutUint32(index.mmap[index.size:index.size+offsetWidth], offset)
	enc.PutUint64(index.mmap[index.size+offsetWidth:index.size+totalWidth], position)
	index.size += totalWidth
	return nil
}

func (index *index) isMaxed() bool {
	return uint64(len(index.mmap)) < index.size+totalWidth
}

func (index *index) Name() string {
	return index.file.Name()
}
