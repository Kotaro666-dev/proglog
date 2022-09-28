package log

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, config Config) (*Log, error) {
	// 読み出しもとがconfigの値を指定していない場合、デフォルト値を設定する
	if config.Segment.MaxStoreBytes == 0 {
		config.Segment.MaxStoreBytes = 1024
	}
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = 1024
	}
	log := &Log{
		Dir:    dir,
		Config: config,
	}
	return log, log.setup()
}

func (log *Log) setup() error {
	files, err := os.ReadDir(log.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offsetString := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()))
		offset, _ := strconv.ParseUint(offsetString, 10, 0)
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsetsは、インデックスとストアの二つの重複を含んで
		// いるので、重複しているものをスキップする
		i++
	}

	// 既存のセグメントがない場合、最初のセグメントを作成する
	if log.segments == nil {
		if err = log.newSegment(
			log.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return err
}

func (log *Log) newSegment(offset uint64) error {
	segment, err := newSegment(log.Dir, offset, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, segment)
	log.activeSegment = segment
	return nil
}
