package log

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"

	api "github/Kotaro666-dev/prolog/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	segment := &segment{
		baseOffset: baseOffset,
		config:     config,
	}
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if segment.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if segment.index, err = newIndex(indexFile, config); err != nil {
		return nil, err
	}
	// 次に追加されるレコードのための準備をする
	if offset, _, err := segment.index.Read(-1); err != nil {
		// インデックスが空の場合、セグメントのbaseOffsetを当てはめる
		segment.nextOffset = baseOffset
	} else {
		// セグメントの最後のオフセットを使う（baseOffset + 相対offsetの和に1を加算する）
		segment.nextOffset = baseOffset + uint64(offset) + 1
	}
	return segment, nil
}

func (segment *segment) Append(record *api.Record) (offset uint64, err error) {
	current := segment.nextOffset
	record.Offset = current
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, position, err := segment.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = segment.index.Write(
		// インデックスのオフセットは、ベースオフセットからの相対
		uint32(segment.nextOffset-segment.baseOffset),
		position,
	); err != nil {
		return 0, err
	}
	segment.nextOffset++
	return current, nil
}

func (segment *segment) Read(offset uint64) (*api.Record, error) {
	// 絶対オフセットから相対オフセットに変換し、関連するインデックスエントリの内容を取得する
	_, position, err := segment.index.Read(int64(offset - segment.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := segment.store.Read(position)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed セグメントが最大サイズに達したか、ストアまたはインデックスへの書き込みが一杯になったかどうかを判定
func (segment *segment) IsMaxed() bool {
	return segment.store.size >= segment.config.Segment.MaxStoreBytes ||
		segment.index.size >= segment.config.Segment.MaxIndexBytes ||
		segment.index.isMaxed()
}

func (segment *segment) Remove() error {
	if err := segment.Close(); err != nil {
		return err
	}
	if err := os.Remove(segment.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(segment.store.Name()); err != nil {
		return err
	}
	return nil
}

func (segment *segment) Close() error {
	if err := segment.index.Close(); err != nil {
		return err
	}
	if err := segment.store.Close(); err != nil {
		return err
	}
	return nil
}
