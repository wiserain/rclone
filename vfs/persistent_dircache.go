package vfs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/vfs/vfsdircache"
)

// persistentDirCache is the VFS-facing contract for restart-safe listings.
// Keeping it private isolates the VFS core from the storage implementation.
type persistentDirCache interface {
	LoadDirectory(context.Context, string, time.Duration) (fs.DirEntries, time.Time, bool, error)
	SaveDirectory(context.Context, string, fs.DirEntries, time.Time) error
	BeginTreeRefresh(string) (vfsdircache.TreeRefreshToken, error)
	AbortTreeRefresh(vfsdircache.TreeRefreshToken)
	ReplaceTree(context.Context, string, dirtree.DirTree, time.Time, vfsdircache.TreeRefreshToken) (vfsdircache.TreeRefreshResult, error)
	InvalidateDirectory(string) error
	InvalidateSubtree(string) error
	ExpireSubtree(string) error
	Stats() rc.Params
	Purge() error
	Close() error
}

func (vfs *VFS) initPersistentDirCache() {
	if !vfs.Opt.DirCachePersist {
		return
	}
	if _, supported := fs.GetPersistentDirCacher(vfs.f); !supported {
		fs.Infof(vfs.f, "Persistent VFS directory cache is not supported by this backend")
		return
	}
	dirCache, err := vfsdircache.New(vfs.ctx, vfs.f, &vfs.Opt)
	if err != nil {
		fs.Errorf(vfs.f, "Failed to open persistent VFS directory cache - disabling: %v", err)
		return
	}
	vfs.dirCache = dirCache
	fs.Infof(vfs.f, "Persistent VFS directory cache is enabled at %q", dirCache.Path())
}

func (vfs *VFS) addPersistentDirCacheStats(out rc.Params) {
	if vfs.dirCache != nil {
		out["persistentDirCache"] = vfs.dirCache.Stats()
	}
}

func (vfs *VFS) closePersistentDirCache() {
	if vfs.dirCache == nil {
		return
	}
	if err := vfs.dirCache.Close(); err != nil {
		fs.Errorf(vfs.f, "Failed to close persistent VFS directory cache: %v", err)
	}
}

func (vfs *VFS) cleanUpPersistentDirCache(cacheErr error) error {
	if vfs.dirCache == nil {
		return cacheErr
	}
	return errors.Join(cacheErr, vfs.dirCache.Purge())
}

// ForgetAll forgets directory entries for this directory and any children.
//
// It does not invalidate or clear the cache of the parent directory. The
// matching persistent directory cache subtree is also invalidated.
//
// It returns true if the directory or any of its children had virtual
// entries so could not be forgotten. Children which didn't have virtual
// entries will be forgotten even if true is returned.
func (d *Dir) ForgetAll() (hasVirtual bool) {
	dirPath := d.Path()
	hasVirtual = d.forgetAllMemory()
	d.invalidatePersistentSubtree(dirPath)
	return hasVirtual
}

// expireAll forgets stale memory and disk listings without superseding a
// concurrently collected remote snapshot.
func (d *Dir) expireAll() (hasVirtual bool) {
	dirPath := d.Path()
	hasVirtual = d.forgetAllMemory()
	if d.vfs.dirCache != nil {
		if err := d.vfs.dirCache.ExpireSubtree(dirPath); err != nil {
			fs.Errorf(dirPath, "Failed to expire persistent VFS directory subtree: %v", err)
		}
	}
	return hasVirtual
}

func (d *Dir) invalidatePersistentDir(dirPath string) {
	if d.vfs.dirCache == nil {
		return
	}
	if err := d.vfs.dirCache.InvalidateDirectory(dirPath); err != nil {
		fs.Errorf(dirPath, "Failed to invalidate persistent VFS directory cache: %v", err)
	}
}

func (d *Dir) invalidatePersistentSubtree(dirPath string) {
	if d.vfs.dirCache == nil {
		return
	}
	if err := d.vfs.dirCache.InvalidateSubtree(dirPath); err != nil {
		fs.Errorf(dirPath, "Failed to invalidate persistent VFS directory subtree: %v", err)
	}
}

// invalidateDirSubtree invalidates memory and the disk subtree while holding
// the target directory lock so an old record cannot be restored between them.
func (d *Dir) invalidateDirSubtree(absPath string) {
	node := d.vfs.root.cachedNode(absPath)
	if dir, ok := node.(*Dir); ok {
		dir.mu.Lock()
		if !dir.read.IsZero() {
			fs.Debugf(dir.path, "invalidating directory cache")
			dir.read = time.Time{}
		}
		dir.invalidatePersistentSubtree(absPath)
		dir.mu.Unlock()
		return
	}
	d.invalidatePersistentSubtree(absPath)
}

func (d *Dir) invalidateDirMemory(absPath string) {
	node := d.vfs.root.cachedNode(absPath)
	if dir, ok := node.(*Dir); ok {
		dir.mu.Lock()
		if !dir.read.IsZero() {
			fs.Debugf(dir.path, "invalidating directory cache")
			dir.read = time.Time{}
		}
		dir.mu.Unlock()
	}
}

func (d *Dir) invalidateDirSubtreeMemory(absPath string) {
	node := d.vfs.root.cachedNode(absPath)
	if dir, ok := node.(*Dir); ok {
		dir.forgetAllMemory()
	}
}

// restorePersistentDirLocked restores d after an in-memory cache miss.
// d.mu must be held.
func (d *Dir) restorePersistentDirLocked() bool {
	if d.vfs.dirCache == nil {
		return false
	}
	entries, refreshedAt, found, err := d.vfs.dirCache.LoadDirectory(
		d.vfs.ctx, d.path, time.Duration(d.vfs.Opt.DirCacheTime),
	)
	if err != nil {
		fs.Errorf(d.path, "Failed to restore persistent VFS directory cache; reading remote: %v", err)
		d.invalidatePersistentSubtree(d.path)
		return false
	}
	if !found {
		return false
	}
	if err = d._readDirFromEntries(entries, nil, time.Time{}); err != nil {
		fs.Errorf(d.path, "Failed to apply persistent VFS directory cache; reading remote: %v", err)
		d.invalidatePersistentSubtree(d.path)
		return false
	}
	d.read = refreshedAt
	d.cleanupTimer.Reset(time.Duration(d.vfs.Opt.DirCacheTime * 2))
	fs.Debugf(d.path, "Restored directory from persistent VFS cache (%v old)", time.Since(refreshedAt))
	return true
}

// savePersistentDirLocked saves a remote-confirmed listing. d.mu must be held.
func (d *Dir) savePersistentDirLocked(entries fs.DirEntries) {
	if d.vfs.dirCache == nil {
		return
	}
	if len(d.virtual) != 0 {
		fs.Debugf(d.path, "Not saving persistent VFS directory cache while virtual entries are pending")
		return
	}
	if err := d.vfs.dirCache.SaveDirectory(d.vfs.ctx, d.path, entries, d.read); err != nil {
		fs.Errorf(d.path, "Failed to save persistent VFS directory cache: %v", err)
	}
}

type persistentTreeRefresh struct {
	dir         *Dir
	dirPath     string
	token       vfsdircache.TreeRefreshToken
	tree        dirtree.DirTree
	refreshedAt time.Time
}

func (d *Dir) startPersistentTreeRefresh(dirPath string) (*persistentTreeRefresh, error) {
	refresh := &persistentTreeRefresh{
		dir:     d,
		dirPath: dirPath,
	}
	if d.vfs.dirCache == nil {
		return refresh, nil
	}
	token, err := d.vfs.dirCache.BeginTreeRefresh(dirPath)
	refresh.token = token
	return refresh, err
}

func (r *persistentTreeRefresh) complete(tree dirtree.DirTree, refreshedAt time.Time) {
	r.tree = tree
	r.refreshedAt = refreshedAt
}

func (r *persistentTreeRefresh) finish(refreshErr *error) {
	if r.dir.vfs.dirCache == nil {
		return
	}
	defer r.dir.vfs.dirCache.AbortTreeRefresh(r.token)
	if *refreshErr != nil || r.tree == nil {
		return
	}
	result, err := r.dir.replacePersistentTree(r.dirPath, r.tree, r.refreshedAt, r.token)
	if err != nil {
		*refreshErr = fmt.Errorf("failed to save persistent VFS directory tree: %w", err)
		return
	}
	r.dir.invalidateTreeRefreshMemory(result)
}

func (d *Dir) replacePersistentTree(dirPath string, tree dirtree.DirTree, refreshedAt time.Time, token vfsdircache.TreeRefreshToken) (vfsdircache.TreeRefreshResult, error) {
	if d.vfs.dirCache == nil {
		return vfsdircache.TreeRefreshResult{}, nil
	}
	if d.hasVirtual() {
		fs.Debugf(d.path, "Not saving persistent VFS directory tree while virtual entries are pending")
		return vfsdircache.TreeRefreshResult{}, nil
	}
	return d.vfs.dirCache.ReplaceTree(d.vfs.ctx, dirPath, tree, refreshedAt, token)
}

func (d *Dir) invalidateTreeRefreshMemory(result vfsdircache.TreeRefreshResult) {
	for _, dirPath := range result.StaleSubtrees {
		d.invalidateDirSubtreeMemory(dirPath)
	}
	for _, dirPath := range result.StaleDirectories {
		d.invalidateDirMemory(dirPath)
	}
}
