package _115 // nolint:revive

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
)

var (
	benchmarkPersistent115Record persistentDirCacheRecord
	benchmarkPersistent115Entry  fs.DirEntry
)

func makeBenchmarkPersistent115Record() persistentDirCacheRecord {
	return persistentDirCacheRecord{
		Version:          persistentDirCacheRecordVersion,
		Kind:             persistent115Object,
		ID:               "1234567890123456789",
		ParentID:         "9876543210987654321",
		Size:             123456789,
		SHA1:             "0123456789abcdef0123456789abcdef01234567",
		PickCode:         "ecjq9ichcb40lzlvx",
		ModTimeUnixNano:  1776297600123456789,
		ModTimeIsPresent: true,
	}
}

func makeBenchmarkPersistent115Object() *Object {
	f := newPersistentDirCacheTestFs()
	return &Object{
		fs:          f,
		remote:      "dir/file.bin",
		hasMetaData: true,
		id:          "1234567890123456789",
		parent:      "9876543210987654321",
		size:        123456789,
		sha1sum:     "0123456789abcdef0123456789abcdef01234567",
		pickCode:    "ecjq9ichcb40lzlvx",
		modTime:     time.Unix(0, 1776297600123456789),
	}
}

func BenchmarkDecodePersistent115Record(b *testing.B) {
	data, err := json.Marshal(makeBenchmarkPersistent115Record())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var record persistentDirCacheRecord
		if err := json.Unmarshal(data, &record); err != nil {
			b.Fatal(err)
		}
		benchmarkPersistent115Record = record
	}
	b.ReportMetric(float64(len(data)), "record-bytes")
}

func BenchmarkDecodePersistent115Entry(b *testing.B) {
	ctx := context.Background()
	object := makeBenchmarkPersistent115Object()
	data, err := object.fs.EncodePersistentDirEntry(ctx, object)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkPersistent115Entry, err = object.fs.DecodePersistentDirEntry(ctx, object.remote, false, data)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(data)), "record-bytes")
}
