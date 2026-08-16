//go:build !plan9 && !js

package vfs

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type persistentTestFs struct {
	fs.Fs
	lastPolicy fs.PersistentDirCachePolicy

	blockListDir string
	listStarted  chan struct{}
	continueList chan struct{}
	listOnce     sync.Once
}

func (f *persistentTestFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if f.listStarted != nil && dir == f.blockListDir {
		f.listOnce.Do(func() {
			close(f.listStarted)
			<-f.continueList
		})
	}
	return f.Fs.List(ctx, dir)
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

func (f *persistentTestFs) EncodePersistentDirEntry(ctx context.Context, entry fs.DirEntry, policy fs.PersistentDirCachePolicy) ([]byte, error) {
	f.lastPolicy = policy
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
	databasePath := vfs1.dirCache.Stats()["path"].(string)
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

	// A memory-only eviction can restore entries from the persistent cache.
	root.forgetAllMemory()
	dirNode, err = root.Stat("dir")
	require.NoError(t, err)
	dir = dirNode.(*Dir)
	fileNode, err = dir.Stat("file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len("persistent contents")), fileNode.Size())

	// Automatic cleanup of a stale listing must invalidate the on-disk snapshot
	// and force the next lookup to observe the changed remote.
	root.mu.Lock()
	root.read = time.Now().Add(-2 * time.Duration(opt.DirCacheTime))
	root.mu.Unlock()
	root.cacheCleanup()
	dirNode, err = root.Stat("dir")
	require.NoError(t, err)
	dir = dirNode.(*Dir)
	_, err = dir.Stat("file.txt")
	assert.ErrorIs(t, err, ENOENT)
}

func TestPersistentDirCacheRecursiveRefreshReplaysChangeNotify(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "dir/file.txt", "contents", t1)
	persistentFs := &persistentTestFs{
		Fs:           r.Fremote,
		blockListDir: "",
		listStarted:  make(chan struct{}),
		continueList: make(chan struct{}),
	}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff
	vfs := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs.Shutdown)

	refreshErr := make(chan error, 1)
	go func() {
		refreshErr <- vfs.root.readDirTree()
	}()
	<-persistentFs.listStarted
	vfs.root.changeNotify("dir/file.txt", fs.EntryObject)
	close(persistentFs.continueList)
	require.NoError(t, <-refreshErr)

	_, _, rootFound, err := vfs.dirCache.LoadDirectory(ctx, "", 24*time.Hour)
	require.NoError(t, err)
	require.True(t, rootFound)
	_, _, dirFound, err := vfs.dirCache.LoadDirectory(ctx, "dir", 24*time.Hour)
	require.NoError(t, err)
	require.False(t, dirFound)

	dirNode := vfs.root.cachedNode("dir")
	dir, ok := dirNode.(*Dir)
	require.True(t, ok)
	dir.mu.RLock()
	defer dir.mu.RUnlock()
	require.True(t, dir.read.IsZero())
}

func TestPersistentDirCacheRejectsConcurrentRecursiveRefresh(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	persistentFs := &persistentTestFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.CacheMode = vfscommon.CacheModeOff
	vfs := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs.Shutdown)

	token, err := vfs.dirCache.BeginTreeRefresh("")
	require.NoError(t, err)
	defer vfs.dirCache.AbortTreeRefresh(token)
	err = vfs.root.readDirTree()
	require.ErrorContains(t, err, "tree refresh is already running")
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

func TestPersistentDirCacheUnsupportedBackendIsDisabled(t *testing.T) {
	r := fstest.NewRun(t)
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.CacheMode = vfscommon.CacheModeOff

	vfs := New(context.Background(), r.Fremote, &opt)
	t.Cleanup(vfs.Shutdown)
	assert.Nil(t, vfs.dirCache)
}

func TestPersistentDirCachePolicy(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "file.txt", "contents", t1)
	persistentFs := &persistentTestFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.NoChecksum = true
	opt.NoModTime = true

	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	require.NoError(t, vfs1.root.readDirTree())
	assert.Equal(t, fs.PersistentDirCachePolicy{NoChecksum: true, NoModTime: true}, persistentFs.lastPolicy)
	identityWithPolicy := vfs1.dirCache.Stats()["identity"]
	vfs1.Shutdown()

	opt.NoChecksum = false
	opt.NoModTime = false
	vfs2 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs2.dirCache)
	assert.NotEqual(t, identityWithPolicy, vfs2.dirCache.Stats()["identity"])
	vfs2.Shutdown()
}

func TestPersistentDirCacheChangeNotifyInvalidatesParent(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "dir/file.txt", "contents", t1)
	persistentFs := &persistentTestFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff

	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	require.NoError(t, vfs1.root.readDirTree())
	root, err := vfs1.Root()
	require.NoError(t, err)
	dirNode, err := root.Stat("dir")
	require.NoError(t, err)
	dir := dirNode.(*Dir)

	remoteObject, err := r.Fremote.NewObject(ctx, "dir/file.txt")
	require.NoError(t, err)
	require.NoError(t, remoteObject.Remove(ctx))
	root.changeNotify("dir/file.txt", fs.EntryObject)
	_, err = dir.Stat("file.txt")
	assert.ErrorIs(t, err, ENOENT)
	vfs1.Shutdown()

	// The remote result read after notification must replace the disk record.
	vfs2 := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs2.Shutdown)
	root, err = vfs2.Root()
	require.NoError(t, err)
	dirNode, err = root.Stat("dir")
	require.NoError(t, err)
	dir = dirNode.(*Dir)
	_, err = dir.Stat("file.txt")
	assert.ErrorIs(t, err, ENOENT)
}

func TestPersistentDirCacheCleanUpPurgesListings(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "file.txt", "contents", t1)
	persistentFs := &persistentTestFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff

	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	require.NoError(t, vfs1.root.readDirTree())
	require.NoError(t, vfs1.CleanUp())
	vfs1.Shutdown()

	remoteObject, err := r.Fremote.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	require.NoError(t, remoteObject.Remove(ctx))
	vfs2 := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs2.Shutdown)
	root, err := vfs2.Root()
	require.NoError(t, err)
	_, err = root.Stat("file.txt")
	assert.ErrorIs(t, err, ENOENT)
}

func TestPersistentDirCacheRCRefreshReplacesSnapshot(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	r.WriteObject(ctx, "stale.txt", "contents", t1)
	persistentFs := &persistentTestFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff

	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	require.NoError(t, vfs1.root.readDirTree())
	remoteObject, err := r.Fremote.NewObject(ctx, "stale.txt")
	require.NoError(t, err)
	require.NoError(t, remoteObject.Remove(ctx))

	out, err := rcRefresh(ctx, rc.Params{
		"fs":        fs.ConfigString(persistentFs),
		"recursive": "true",
	})
	require.NoError(t, err)
	assert.Equal(t, "OK", out["result"].(map[string]string)[""])
	vfs1.Shutdown()

	vfs2 := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs2.Shutdown)
	root, err := vfs2.Root()
	require.NoError(t, err)
	_, err = root.Stat("stale.txt")
	assert.ErrorIs(t, err, ENOENT)
}

func TestPersistentDirCacheRCRefreshPersistsEmptyDirectory(t *testing.T) {
	ctx := context.Background()
	oldCacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, config.SetCacheDir(oldCacheDir))
	})

	r := fstest.NewRun(t)
	require.NoError(t, r.Fremote.Mkdir(ctx, "empty"))
	persistentFs := &persistentTestFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.DirCachePersist = true
	opt.DirCacheTime = fs.Duration(24 * time.Hour)
	opt.CacheMode = vfscommon.CacheModeOff

	vfs1 := New(ctx, persistentFs, &opt)
	require.NotNil(t, vfs1.dirCache)
	out, err := rcRefresh(ctx, rc.Params{
		"fs":        fs.ConfigString(persistentFs),
		"recursive": "true",
	})
	require.NoError(t, err)
	assert.Equal(t, "OK", out["result"].(map[string]string)[""])
	assert.Equal(t, 2, vfs1.dirCache.Stats()["directories"])
	vfs1.Shutdown()

	// A persistent empty listing must hide changes made after the snapshot.
	r.WriteObject(ctx, "empty/new.txt", "new contents", t1)

	vfs2 := New(ctx, persistentFs, &opt)
	t.Cleanup(vfs2.Shutdown)
	root, err := vfs2.Root()
	require.NoError(t, err)
	dirNode, err := root.Stat("empty")
	require.NoError(t, err)
	empty, ok := dirNode.(*Dir)
	require.True(t, ok)
	_, err = empty.Stat("new.txt")
	assert.ErrorIs(t, err, ENOENT)
	assert.GreaterOrEqual(t, vfs2.dirCache.Stats()["hits"].(uint64), uint64(2))
}
