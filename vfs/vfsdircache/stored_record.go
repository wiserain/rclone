//go:build !plan9 && !js

package vfsdircache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	storedRecordRaw byte = iota
	storedRecordZstd

	storedRecordCompressionThreshold = 1024
	maxStoredRecordSize              = 256 * 1024 * 1024
)

var (
	storedRecordMagic = []byte{'r', 'c', 'd', 'c'}

	storedRecordCodecsOnce sync.Once
	storedRecordEncoder    *zstd.Encoder
	storedRecordDecoder    *zstd.Decoder
	storedRecordCodecsErr  error
)

func getStoredRecordCodecs() (*zstd.Encoder, *zstd.Decoder, error) {
	storedRecordCodecsOnce.Do(func() {
		storedRecordEncoder, storedRecordCodecsErr = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(true),
		)
		if storedRecordCodecsErr != nil {
			return
		}
		storedRecordDecoder, storedRecordCodecsErr = zstd.NewReader(nil,
			zstd.WithDecoderMaxMemory(maxStoredRecordSize),
		)
	})
	return storedRecordEncoder, storedRecordDecoder, storedRecordCodecsErr
}

func appendStoredRecordHeader(dst []byte, codec byte, size int) []byte {
	dst = append(dst, storedRecordMagic...)
	dst = append(dst, codec)
	return binary.AppendUvarint(dst, uint64(size))
}

func encodeStoredDirectoryRecord(data []byte) ([]byte, error) {
	if len(data) > maxStoredRecordSize {
		return nil, fmt.Errorf("persistent directory record is too large: %d bytes", len(data))
	}
	if len(data) >= storedRecordCompressionThreshold {
		encoder, _, err := getStoredRecordCodecs()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize persistent directory record compression: %w", err)
		}
		header := appendStoredRecordHeader(nil, storedRecordZstd, len(data))
		compressed := encoder.EncodeAll(data, header)
		if len(compressed) < len(data) {
			return compressed, nil
		}
	}
	header := appendStoredRecordHeader(nil, storedRecordRaw, len(data))
	return append(header, data...), nil
}

func decodeStoredDirectoryRecord(data []byte) (directoryRecord, error) {
	// Records written before the storage envelope was introduced contain the
	// canonical directory record directly.
	if !bytes.HasPrefix(data, storedRecordMagic) {
		return decodeDirectoryRecord(data)
	}
	decoder := directoryRecordDecoder{data: data, offset: len(storedRecordMagic)}
	codec, err := decoder.readByte()
	if err != nil {
		return directoryRecord{}, fmt.Errorf("invalid persistent directory storage envelope: %w", err)
	}
	size, err := decoder.readUvarint()
	if err != nil {
		return directoryRecord{}, fmt.Errorf("invalid persistent directory storage envelope: %w", err)
	}
	if size > maxStoredRecordSize {
		return directoryRecord{}, fmt.Errorf("persistent directory record is too large: %d bytes", size)
	}
	payload := data[decoder.offset:]
	var raw []byte
	switch codec {
	case storedRecordRaw:
		if uint64(len(payload)) != size {
			return directoryRecord{}, errors.New("persistent raw directory record size mismatch")
		}
		raw = payload
	case storedRecordZstd:
		_, zstdDecoder, codecErr := getStoredRecordCodecs()
		if codecErr != nil {
			return directoryRecord{}, fmt.Errorf("failed to initialize persistent directory record decompression: %w", codecErr)
		}
		raw, err = zstdDecoder.DecodeAll(payload, make([]byte, 0, int(size)))
		if err != nil {
			return directoryRecord{}, fmt.Errorf("failed to decompress persistent directory record: %w", err)
		}
		if uint64(len(raw)) != size {
			return directoryRecord{}, errors.New("persistent compressed directory record size mismatch")
		}
	default:
		return directoryRecord{}, fmt.Errorf("unknown persistent directory storage codec %d", codec)
	}
	return decodeDirectoryRecord(raw)
}
