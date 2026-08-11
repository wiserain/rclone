//go:build !plan9 && !js

package vfs

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type persistentTestFs struct {
	fs.Fs
}

type persistentTestRecord struct {
	IsDir   bool
	Size    int64
	ModTime time.Time
	ID      string
	Parent  string
}

func (f *persistentTestFs) PersistentDirCacheIdentity() string {
	return "vfs-persistent-test"
}

func (f *persistentTestFs) EncodePersistentDirEntry(ctx context.Context, entry fs.DirEntry) ([]byte, error) {
	record := persistentTestRecord{
		Size:    entry.Size(),
		ModTime: entry.ModTime(ctx),
	}
	if ider, ok := entry.(fs.IDer); ok {
		record.ID = ider.ID()
	}
	if parentIDer, ok := entry.(fs.ParentIDer); ok {
		record.Parent = parentIDer.ParentID()
	}
	_, record.IsDir = entry.(fs.Directory)
	return json.Marshal(record)
}

func (f *persistentTestFs) DecodePersistentDirEntry(_ context.Context, remote string, isDir bool, data []byte) (fs.DirEntry, error) {
	var record persistentTestRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	if isDir {
		return fs.NewDir(remote, record.ModTime).
			SetSize(record.Size).
			SetID(record.ID).
			SetParentID(record.Parent), nil
	}
	return object.NewMemoryObject(remote, record.ModTime, make([]byte, record.Size)).SetFs(f), nil
}

func TestPersistentDirCacheSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "dir/file.txt", "persistent contents", t1)

	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff

	persistentFs := &persistentTestFs{Fs: r.Fremote}
	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	require.NoError(t, vfs1.root.readDirTree())
	databasePath := vfs1.dirCache.Path()
	require.FileExists(t, databasePath)
	assert.Equal(t, "dircache.db", filepath.Base(databasePath))
	assert.Contains(t, filepath.ToSlash(databasePath), "/vfsDirCache/")
	vfs1.Shutdown()

	// Change the remote after the snapshot. The second VFS must still see the
	// snapshotted entry, proving it did not need a remote directory traversal.
	remoteObject, err := r.Fremote.NewObject(ctx, "dir/file.txt")
	require.NoError(t, err)
	require.NoError(t, remoteObject.Remove(ctx))

	vfs2 := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs2.Shutdown)

	root, err := vfs2.Root()
	require.NoError(t, err)
	dirNode, err := root.Stat("dir")
	require.NoError(t, err)
	dir, ok := dirNode.(*Dir)
	require.True(t, ok)
	fileNode, err := dir.Stat("file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len("persistent contents")), fileNode.Size())

	stats := vfs2.dirCache.Stats()
	assert.GreaterOrEqual(t, stats["hits"].(uint64), uint64(2))

	// Automatic memory cleanup must not erase the restart-safe records.
	root.forgetAllMemory()
	dirNode, err = root.Stat("dir")
	require.NoError(t, err)
	dir = dirNode.(*Dir)
	fileNode, err = dir.Stat("file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len("persistent contents")), fileNode.Size())

	// An explicit forget must invalidate the on-disk snapshot and force the
	// next lookup to observe the changed remote.
	root.ForgetAll()
	dirNode, err = root.Stat("dir")
	require.NoError(t, err)
	dir = dirNode.(*Dir)
	_, err = dir.Stat("file.txt")
	assert.ErrorIs(t, err, ENOENT)
}

func TestPersistentDirCacheDropsRemovedChildSubtree(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "dir/stale.txt", "old contents", t1)
	// The sibling deliberately sorts between the exact "dir" DB key and the
	// "dir/..." descendant keys. Subtree invalidation must skip over neither.
	r.WriteObject(ctx, "dir-old/keep.txt", "keep contents", t1)

	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff

	persistentFs := &persistentTestFs{Fs: r.Fremote}
	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	require.NoError(t, vfs1.root.readDirTree())
	vfs1.Shutdown()

	remoteObject, err := r.Fremote.NewObject(ctx, "dir/stale.txt")
	require.NoError(t, err)
	require.NoError(t, remoteObject.Remove(ctx))
	require.NoError(t, r.Fremote.Rmdir(ctx, "dir"))

	vfs2 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs2.dirCache)
	// Saving the parent without "dir" must remove the old child's records.
	require.NoError(t, vfs2.root.readDir())
	require.NoError(t, r.Fremote.Mkdir(ctx, "dir"))
	// Re-add the same path without recursively reading it.
	require.NoError(t, vfs2.root.readDir())
	vfs2.Shutdown()

	vfs3 := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs3.Shutdown)
	root, err := vfs3.Root()
	require.NoError(t, err)
	dirNode, err := root.Stat("dir")
	require.NoError(t, err)
	dir, ok := dirNode.(*Dir)
	require.True(t, ok)
	_, err = dir.Stat("stale.txt")
	assert.ErrorIs(t, err, ENOENT)
}
