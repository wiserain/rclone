package _115 // nolint:revive

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/115/dircache"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPersistentDirCacheTestFs() *Fs {
	f := &Fs{
		userID:       "user-1",
		rootFolderID: "root-1",
	}
	f.dirCache = dircache.New("", f.rootFolderID, f)
	return f
}

func TestPersistentDirCacheIdentity(t *testing.T) {
	f := newPersistentDirCacheTestFs()
	assert.Equal(t, "user_id=user-1;root_folder_id=root-1;share=false;share_code=", f.PersistentDirCacheIdentity())

	f.isShare = true
	f.opt.ShareCode = "share-1"
	assert.Equal(t, "user_id=user-1;root_folder_id=root-1;share=true;share_code=share-1", f.PersistentDirCacheIdentity())
}

func TestPersistentDirCacheObjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDirCacheTestFs()
	wantTime := time.Unix(123, 456)
	want := &Object{
		fs:          f,
		remote:      "dir/file.bin",
		hasMetaData: true,
		id:          "file-1",
		parent:      "dir-1",
		size:        42,
		sha1sum:     "abcdef",
		pickCode:    "pick-1",
		modTime:     wantTime,
	}

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	gotEntry, err := f.DecodePersistentDirEntry(ctx, want.remote, false, data)
	require.NoError(t, err)
	got, ok := gotEntry.(*Object)
	require.True(t, ok)
	assert.Equal(t, want.remote, got.remote)
	assert.Equal(t, want.id, got.id)
	assert.Equal(t, want.parent, got.parent)
	assert.Equal(t, want.size, got.size)
	assert.Equal(t, want.sha1sum, got.sha1sum)
	assert.Equal(t, want.pickCode, got.pickCode)
	assert.Equal(t, wantTime, got.modTime)
	assert.True(t, got.hasMetaData)
	assert.NotNil(t, got.durlMu)
}

func TestPersistentDirCacheDirectoryRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDirCacheTestFs()
	wantTime := time.Unix(789, 123)
	want := fs.NewDir("dir", wantTime).
		SetSize(12).
		SetItems(3).
		SetID("dir-1").
		SetParentID("root-1")

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	gotEntry, err := f.DecodePersistentDirEntry(ctx, want.Remote(), true, data)
	require.NoError(t, err)
	got, ok := gotEntry.(fs.Directory)
	require.True(t, ok)
	assert.Equal(t, want.Remote(), got.Remote())
	assert.Equal(t, want.ID(), got.ID())
	assert.Equal(t, want.ParentID(), got.(fs.ParentIDer).ParentID())
	assert.Equal(t, wantTime, got.ModTime(ctx))
	assert.Equal(t, want.Size(), got.Size())
	assert.Equal(t, want.Items(), got.Items())
	id, found := f.dirCache.Get("dir")
	assert.True(t, found)
	assert.Equal(t, "dir-1", id)
}

func TestPersistentDirCacheRejectsInvalidRecord(t *testing.T) {
	f := newPersistentDirCacheTestFs()
	_, err := f.DecodePersistentDirEntry(context.Background(), "file", false, []byte(`{"v":99}`))
	assert.ErrorContains(t, err, "unsupported 115 persistent directory cache record version")
}

func TestPersistentDirCachePolicyOmitsObjectMetadata(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDirCacheTestFs()
	object := &Object{
		fs:          f,
		remote:      "dir/file.bin",
		hasMetaData: true,
		id:          "file-1",
		parent:      "dir-1",
		size:        42,
		sha1sum:     "abcdef",
		pickCode:    "pick-1",
		modTime:     time.Unix(123, 456),
	}
	data, err := object.fs.EncodePersistentDirEntry(ctx, object, fs.PersistentDirCachePolicy{
		NoChecksum: true,
		NoModTime:  true,
	})
	require.NoError(t, err)
	var record persistentDirCacheRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Empty(t, record.SHA1)
	assert.False(t, record.ModTimeIsPresent)
	assert.Zero(t, record.ModTimeUnixNano)
	assert.Equal(t, object.pickCode, record.PickCode)

	directory := fs.NewDir("dir", time.Unix(123, 456)).SetID("dir-1")
	data, err = object.fs.EncodePersistentDirEntry(ctx, directory, fs.PersistentDirCachePolicy{NoModTime: true})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &record))
	assert.True(t, record.ModTimeIsPresent)
	assert.Equal(t, directory.ModTime(ctx).UnixNano(), record.ModTimeUnixNano)
}
