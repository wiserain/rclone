package drive

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	driveapi "google.golang.org/api/drive/v3"
)

func newPersistentDriveTestFs() *Fs {
	f := &Fs{
		rootFolderID:    "root-1",
		dirResourceKeys: new(sync.Map),
		svc:             &driveapi.Service{BasePath: "https://drive.test/"},
	}
	f.dirCache = dircache.New("", f.rootFolderID, f)
	return f
}

func persistentDriveTestBase(f *Fs) baseObject {
	resourceKey := "resource-1"
	metadata := fs.Metadata{"key": "value"}
	return baseObject{
		fs:           f,
		remote:       "dir/item",
		id:           "item-1",
		modifiedDate: "2026-08-11T00:00:00Z",
		mimeType:     "application/octet-stream",
		bytes:        42,
		parents:      []string{"parent-1"},
		resourceKey:  &resourceKey,
		metadata:     &metadata,
	}
}

func TestPersistentDirCacheIdentity(t *testing.T) {
	f := newPersistentDriveTestFs()
	identity := f.PersistentDirCacheIdentity()
	assert.Contains(t, identity, `"root_folder_id":"root-1"`)

	f.opt.SharedWithMe = true
	assert.NotEqual(t, identity, f.PersistentDirCacheIdentity())
	f.opt.SharedWithMe = false
	f.rootFolderID = "root-2"
	assert.NotEqual(t, identity, f.PersistentDirCacheIdentity())
}

func TestPersistentDirCacheRegularObjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDriveTestFs()
	want := &Object{
		baseObject: persistentDriveTestBase(f),
		md5sum:     "md5",
		sha1sum:    "sha1",
		sha256sum:  "sha256",
		v2Download: true,
	}

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	entry, err := f.DecodePersistentDirEntry(ctx, want.remote, false, data)
	require.NoError(t, err)
	got, ok := entry.(*Object)
	require.True(t, ok)
	assert.Equal(t, want.id, got.id)
	assert.Equal(t, want.parents, got.parents)
	assert.Equal(t, want.md5sum, got.md5sum)
	assert.Equal(t, want.sha1sum, got.sha1sum)
	assert.Equal(t, want.sha256sum, got.sha256sum)
	assert.Equal(t, "https://drive.test/files/item-1?alt=media", got.url)
	assert.Equal(t, *want.resourceKey, *got.resourceKey)
	assert.Equal(t, *want.metadata, *got.metadata)
}

func TestPersistentDirCacheDirectoryRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDriveTestFs()
	want := &Directory{baseObject: persistentDriveTestBase(f)}
	want.mimeType = driveFolderType

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	entry, err := f.DecodePersistentDirEntry(ctx, want.remote, true, data)
	require.NoError(t, err)
	got, ok := entry.(*Directory)
	require.True(t, ok)
	assert.Equal(t, want.id, got.id)
	id, found := f.dirCache.Get(want.remote)
	assert.True(t, found)
	assert.Equal(t, want.id, id)
	resourceKey, found := f.dirResourceKeys.Load(want.id)
	assert.True(t, found)
	assert.Equal(t, *want.resourceKey, resourceKey)
}

func TestPersistentDirCacheDocumentRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDriveTestFs()
	want := &documentObject{
		baseObject:       persistentDriveTestBase(f),
		url:              "https://drive.test/export/item-1",
		documentMimeType: "application/vnd.google-apps.document",
		extLen:           4,
	}

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	entry, err := f.DecodePersistentDirEntry(ctx, want.remote, false, data)
	require.NoError(t, err)
	got, ok := entry.(*documentObject)
	require.True(t, ok)
	assert.Equal(t, want.url, got.url)
	assert.Equal(t, want.documentMimeType, got.documentMimeType)
	assert.Equal(t, want.extLen, got.extLen)
}

func TestPersistentDirCacheLinkRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDriveTestFs()
	want := &linkObject{
		baseObject: persistentDriveTestBase(f),
		content:    []byte("link contents"),
		extLen:     5,
	}

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	entry, err := f.DecodePersistentDirEntry(ctx, want.remote, false, data)
	require.NoError(t, err)
	got, ok := entry.(*linkObject)
	require.True(t, ok)
	assert.Equal(t, want.content, got.content)
	assert.Equal(t, want.extLen, got.extLen)
}

func TestPersistentDirCacheRejectsKindMismatch(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDriveTestFs()
	want := &Directory{baseObject: persistentDriveTestBase(f)}
	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{})
	require.NoError(t, err)
	_, err = f.DecodePersistentDirEntry(ctx, want.remote, false, data)
	assert.ErrorContains(t, err, "kind mismatch")
}

func TestPersistentDirCachePolicyOmitsObjectMetadata(t *testing.T) {
	ctx := context.Background()
	f := newPersistentDriveTestFs()
	want := &Object{
		baseObject: persistentDriveTestBase(f),
		md5sum:     "md5",
		sha1sum:    "sha1",
		sha256sum:  "sha256",
	}

	data, err := f.EncodePersistentDirEntry(ctx, want, fs.PersistentDirCachePolicy{
		NoChecksum: true,
		NoModTime:  true,
	})
	require.NoError(t, err)
	var record persistentDriveRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Empty(t, record.ModifiedDate)
	assert.Empty(t, record.MD5Sum)
	assert.Empty(t, record.SHA1Sum)
	assert.Empty(t, record.SHA256Sum)
	assert.Equal(t, want.id, record.ID)

	directory := &Directory{baseObject: persistentDriveTestBase(f)}
	data, err = f.EncodePersistentDirEntry(ctx, directory, fs.PersistentDirCachePolicy{NoModTime: true})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, directory.modifiedDate, record.ModifiedDate)
}
