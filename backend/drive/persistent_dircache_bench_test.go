package drive

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rclone/rclone/fs"
)

var (
	benchmarkPersistentDriveData   []byte
	benchmarkPersistentDriveRecord persistentDriveRecord
	benchmarkPersistentDriveEntry  fs.DirEntry
)

func makeBenchmarkPersistentDriveRecord() persistentDriveRecord {
	return persistentDriveRecord{
		Version:      persistentDirCacheRecordVersion,
		Kind:         persistentDriveObject,
		ID:           "1AbCdEfGhIjKlMnOpQrStUvWxYz012345",
		ModifiedDate: "2026-08-16T00:00:00.123Z",
		MimeType:     "application/pdf",
		Bytes:        123456789,
		Parents:      []string{"0AbCdEfGhIjKlMnOpQrStUvWxYz987654"},
		ResourceKey:  "0-resource-key-example",
		Metadata: fs.Metadata{
			"description": "persistent directory cache benchmark",
			"mode":        "0644",
		},
		MD5Sum: "0123456789abcdef0123456789abcdef",
	}
}

func makeBenchmarkPersistentDriveObject() *Object {
	f := newPersistentDriveTestFs()
	base := persistentDriveTestBase(f)
	base.id = "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"
	base.modifiedDate = "2026-08-16T00:00:00.123Z"
	base.mimeType = "application/pdf"
	base.bytes = 123456789
	base.parents = []string{"0AbCdEfGhIjKlMnOpQrStUvWxYz987654"}
	return &Object{
		baseObject: base,
		md5sum:     "0123456789abcdef0123456789abcdef",
	}
}

func BenchmarkEncodePersistentDriveRecord(b *testing.B) {
	record := makeBenchmarkPersistentDriveRecord()
	data, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkPersistentDriveData, err = json.Marshal(record)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(data)), "record-bytes")
}

func BenchmarkDecodePersistentDriveRecord(b *testing.B) {
	data, err := json.Marshal(makeBenchmarkPersistentDriveRecord())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var record persistentDriveRecord
		if err := json.Unmarshal(data, &record); err != nil {
			b.Fatal(err)
		}
		benchmarkPersistentDriveRecord = record
	}
	b.ReportMetric(float64(len(data)), "record-bytes")
}

func BenchmarkEncodePersistentDriveEntry(b *testing.B) {
	ctx := context.Background()
	object := makeBenchmarkPersistentDriveObject()
	data, err := object.fs.EncodePersistentDirEntry(ctx, object, fs.PersistentDirCachePolicy{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkPersistentDriveData, err = object.fs.EncodePersistentDirEntry(ctx, object, fs.PersistentDirCachePolicy{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(data)), "record-bytes")
}

func BenchmarkDecodePersistentDriveEntry(b *testing.B) {
	ctx := context.Background()
	object := makeBenchmarkPersistentDriveObject()
	data, err := object.fs.EncodePersistentDirEntry(ctx, object, fs.PersistentDirCachePolicy{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkPersistentDriveEntry, err = object.fs.DecodePersistentDirEntry(ctx, object.remote, false, data)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(data)), "record-bytes")
}
