//go:build !plan9 && !js

package vfsdircache

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockobject"
)

type benchmarkBackendRecord struct {
	Version      int               `json:"version"`
	Kind         string            `json:"kind"`
	ID           string            `json:"id"`
	ModifiedDate string            `json:"modified_date"`
	MimeType     string            `json:"mime_type"`
	Bytes        int64             `json:"bytes"`
	Parents      []string          `json:"parents"`
	ResourceKey  string            `json:"resource_key"`
	MD5Sum       string            `json:"md5"`
	Metadata     map[string]string `json:"metadata"`
}

type benchmarkCodec struct{}

func (benchmarkCodec) EncodePersistentDirEntry(context.Context, fs.DirEntry) ([]byte, error) {
	panic("not used by benchmark")
}

func (benchmarkCodec) DecodePersistentDirEntry(_ context.Context, remote string, isDir bool, data []byte) (fs.DirEntry, error) {
	var record benchmarkBackendRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	if isDir {
		return fs.NewDir(remote, time.Time{}).SetID(record.ID), nil
	}
	return mockobject.New(remote), nil
}

var (
	benchmarkRecordSink  directoryRecord
	benchmarkEntriesSink fs.DirEntries
)

func makeBenchmarkDirectoryRecord(entryCount int) ([]byte, []entryRecord) {
	const dir = "GDRIVE/READING/만화/완결A/가"
	entries := make([]entryRecord, entryCount)
	for i := range entries {
		isDir := i%12 == 0
		kind := "object"
		entryKind := entryObject
		if isDir {
			kind = "directory"
			entryKind = entryDir
		}
		remote := fmt.Sprintf("%s/작품-%04d/파일-%04d.cbz", dir, i/12, i)
		backendData, err := json.Marshal(benchmarkBackendRecord{
			Version:      1,
			Kind:         kind,
			ID:           fmt.Sprintf("1AbCdEfGhIjKlMnOpQrStUvWxYz%08d", i),
			ModifiedDate: "2026-08-16T00:00:00.000Z",
			MimeType:     "application/octet-stream",
			Bytes:        int64(i+1) * 1024 * 1024,
			Parents:      []string{"0AParentDriveIdentifier0123456789"},
			ResourceKey:  fmt.Sprintf("resource-key-%08d", i),
			MD5Sum:       "d41d8cd98f00b204e9800998ecf8427e",
			Metadata: map[string]string{
				"description": "synthetic persistent directory cache benchmark entry",
			},
		})
		if err != nil {
			panic(err)
		}
		entries[i] = entryRecord{
			Kind:        entryKind,
			Remote:      remote,
			BackendData: backendData,
		}
		if isDir {
			entries[i].ID = fmt.Sprintf("1AbCdEfGhIjKlMnOpQrStUvWxYz%08d", i)
		}
	}
	record := directoryRecord{
		Schema:              recordSchema,
		Path:                dir,
		RefreshedAtUnixNano: 1776297600000000000,
		Entries:             entries,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(record); err != nil {
		panic(err)
	}
	return buf.Bytes(), entries
}

func benchmarkEntryCounts(b *testing.B, fn func(*testing.B, int)) {
	b.Helper()
	for _, entryCount := range []int{10, 100, 1000, 5000} {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			fn(b, entryCount)
		})
	}
}

func BenchmarkDecodeDirectoryRecord(b *testing.B) {
	benchmarkEntryCounts(b, func(b *testing.B, entryCount int) {
		data, _ := makeBenchmarkDirectoryRecord(entryCount)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			record, err := decodeDirectoryRecord(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkRecordSink = record
		}
		b.ReportMetric(float64(len(data))/float64(entryCount), "bytes/entry")
	})
}

func BenchmarkRestoreEntries(b *testing.B) {
	benchmarkEntryCounts(b, func(b *testing.B, entryCount int) {
		_, records := makeBenchmarkDirectoryRecord(entryCount)
		store := &Store{codec: benchmarkCodec{}}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			entries, err := store.restoreEntries(ctx, records)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkEntriesSink = entries
		}
	})
}

func BenchmarkDecodeAndRestoreDirectory(b *testing.B) {
	benchmarkEntryCounts(b, func(b *testing.B, entryCount int) {
		data, _ := makeBenchmarkDirectoryRecord(entryCount)
		store := &Store{codec: benchmarkCodec{}}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			record, err := decodeDirectoryRecord(data)
			if err != nil {
				b.Fatal(err)
			}
			entries, err := store.restoreEntries(ctx, record.Entries)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkEntriesSink = entries
		}
		b.ReportMetric(float64(len(data))/float64(entryCount), "bytes/entry")
	})
}
