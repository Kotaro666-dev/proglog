package log

import (
	"fmt"
	api "github/Kotaro666-dev/prolog/api/v1"
	"io"
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

type originReader struct {
	*store
	offset int64
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

func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	highestOffset, err := log.highestOffset()
	if err != nil {
		return 0, err
	}

	if log.activeSegment.IsMaxed() {
		err = log.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	offset, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	return offset, err
}

func (log *Log) Read(offset uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()

	var s *segment
	for _, segment := range log.segments {
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}
	return s.Read(offset)
}

// Close セグメントをすべてクローズします
func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()

	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove ログをクローズして、そのデータを削除します
func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Dir)
}

// Reset ログを削除して、置き換える新たなログを作成します
func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.highestOffset()
}

func (log *Log) highestOffset() (uint64, error) {
	offset := log.segments[len(log.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}
	return offset - 1, nil
}

// Truncate 最大オフセットがlowestよりも小さいセグメントをすべて削除します
// ディスク容量は無限ではないため、定期的にTruncateを呼んで、それまでに処理したデータで不要になった古いセグメントを削除します
func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	var segments []*segment
	for _, segment := range log.segments {
		if segment.nextOffset <= lowest+1 {
			if err := segment.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, segment)
	}
	log.segments = segments
	return nil
}

// Reader 合意形成の連携を実装し、スナップショットをサポートし、ログの復旧をサポートする必要がある場合、この機能が必要
func (log *Log) Reader() io.Reader {
	log.mu.RLock()
	defer log.mu.RUnlock()

	readers := make([]io.Reader, len(log.segments))
	for i, segment := range log.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

func (originReader *originReader) Read(position []byte) (int, error) {
	n, err := originReader.ReadAt(position, originReader.offset)
	originReader.offset += int64(n)
	return n, err
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
