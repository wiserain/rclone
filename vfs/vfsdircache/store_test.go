//go:build !plan9 && !js

package vfsdircache

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectoryRecordRoundTrip(t *testing.T) {
	want := directoryRecord{
		Schema:              recordSchema,
		Path:                "parent/한글",
		RefreshedAtUnixNano: 1776297600123456789,
		Entries: []entryRecord{
			{Kind: entryDir, Remote: "parent/한글/dir", ID: "directory-id", BackendData: []byte(`{"kind":"drive#file"}`)},
			{Kind: entryObject, Remote: "parent/한글/file.txt", BackendData: []byte{0, 1, 2, 255}},
		},
	}

	got, err := decodeDirectoryRecord(encodeDirectoryRecord(want))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestDirectoryRecordRejectsTruncation(t *testing.T) {
	record := directoryRecord{
		Schema:              recordSchema,
		Path:                "parent",
		RefreshedAtUnixNano: 1776297600123456789,
		Entries: []entryRecord{
			{Kind: entryDir, Remote: "parent/dir", ID: "directory-id", BackendData: []byte("backend data")},
		},
	}
	data := encodeDirectoryRecord(record)

	for length := range len(data) {
		_, err := decodeDirectoryRecord(data[:length])
		require.Error(t, err, "length %d", length)
	}
}

func TestDirectoryRecordRejectsInvalidData(t *testing.T) {
	record := directoryRecord{Schema: recordSchema, Path: "parent"}
	data := append(encodeDirectoryRecord(record), 0)
	_, err := decodeDirectoryRecord(data)
	require.ErrorContains(t, err, "trailing data")

	data = binary.AppendUvarint(nil, recordSchema)
	data = binary.AppendVarint(data, 0)
	data = appendRecordString(data, "parent")
	data = binary.AppendUvarint(data, ^uint64(0))
	_, err = decodeDirectoryRecord(data)
	require.ErrorContains(t, err, "invalid entry count")
}
