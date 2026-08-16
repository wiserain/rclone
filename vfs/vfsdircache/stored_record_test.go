//go:build !plan9 && !js

package vfsdircache

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func testStoredDirectoryRecord(entryCount int) directoryRecord {
	record := directoryRecord{
		Schema:              recordSchema,
		Path:                "parent/한글",
		RefreshedAtUnixNano: 1776297600123456789,
		Entries:             make([]entryRecord, entryCount),
	}
	for i := range record.Entries {
		record.Entries[i] = entryRecord{
			Kind:        entryObject,
			Remote:      "parent/한글/repeated-file-name.txt",
			BackendData: []byte(`{"kind":"drive#file","mime_type":"application/octet-stream"}`),
		}
	}
	return record
}

func TestStoredDirectoryRecordRoundTrip(t *testing.T) {
	for _, entryCount := range []int{0, 100} {
		want := testStoredDirectoryRecord(entryCount)
		stored, err := encodeStoredDirectoryRecord(encodeDirectoryRecord(want))
		require.NoError(t, err)
		got, err := decodeStoredDirectoryRecord(stored)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestStoredDirectoryRecordUsesAdaptiveCodec(t *testing.T) {
	small, err := encodeStoredDirectoryRecord(encodeDirectoryRecord(testStoredDirectoryRecord(0)))
	require.NoError(t, err)
	require.Equal(t, storedRecordRaw, small[len(storedRecordMagic)])

	large, err := encodeStoredDirectoryRecord(encodeDirectoryRecord(testStoredDirectoryRecord(100)))
	require.NoError(t, err)
	require.Equal(t, storedRecordZstd, large[len(storedRecordMagic)])

	incompressible := testStoredDirectoryRecord(1)
	incompressible.Entries[0].BackendData = make([]byte, 4096)
	_, err = rand.New(rand.NewSource(1)).Read(incompressible.Entries[0].BackendData)
	require.NoError(t, err)
	stored, err := encodeStoredDirectoryRecord(encodeDirectoryRecord(incompressible))
	require.NoError(t, err)
	require.Equal(t, storedRecordRaw, stored[len(storedRecordMagic)])
}

func TestStoredDirectoryRecordReadsLegacyRawRecord(t *testing.T) {
	want := testStoredDirectoryRecord(1)
	got, err := decodeStoredDirectoryRecord(encodeDirectoryRecord(want))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestStoredDirectoryRecordRejectsInvalidEnvelope(t *testing.T) {
	tests := map[string][]byte{
		"truncated":     append([]byte(nil), storedRecordMagic...),
		"unknown codec": appendStoredRecordHeader(nil, 99, 0),
		"raw mismatch":  append(appendStoredRecordHeader(nil, storedRecordRaw, 2), 1),
		"too large":     binary.AppendUvarint(append(append([]byte(nil), storedRecordMagic...), storedRecordRaw), maxStoredRecordSize+1),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := decodeStoredDirectoryRecord(data)
			require.Error(t, err)
		})
	}
}

func TestStoredDirectoryRecordRejectsCompressedSizeMismatch(t *testing.T) {
	raw := encodeDirectoryRecord(testStoredDirectoryRecord(100))
	encoder, _, err := getStoredRecordCodecs()
	require.NoError(t, err)
	stored := encoder.EncodeAll(raw, appendStoredRecordHeader(nil, storedRecordZstd, len(raw)+1))
	_, err = decodeStoredDirectoryRecord(stored)
	require.ErrorContains(t, err, "size mismatch")
}

func TestStoredDirectoryRecordRejectsCorruptCompressedData(t *testing.T) {
	stored, err := encodeStoredDirectoryRecord(encodeDirectoryRecord(testStoredDirectoryRecord(100)))
	require.NoError(t, err)
	require.Equal(t, storedRecordZstd, stored[len(storedRecordMagic)])
	stored[len(stored)-1] ^= 0xff
	_, err = decodeStoredDirectoryRecord(stored)
	require.ErrorContains(t, err, "decompress")
}
