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
	ReplaceTree(context.Context, string, dirtree.DirTree, time.Time, uint64) error
	InvalidateDirectory(string) error
	InvalidateSubtree(string) error
	MutationVersion() uint64
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
	vfs.dirDB = dirCache
	fs.Infof(vfs.f, "Persistent VFS directory cache is enabled at %q", dirCache.Path())
}

func (vfs *VFS) addPersistentDirCacheStats(out rc.Params) {
	if vfs.dirDB != nil {
		out["persistentDirCache"] = vfs.dirDB.Stats()
	}
}

func (vfs *VFS) closePersistentDirCache() {
	if vfs.dirDB == nil {
		return
	}
	if err := vfs.dirDB.Close(); err != nil {
		fs.Errorf(vfs.f, "Failed to close persistent VFS directory cache: %v", err)
	}
}

func (vfs *VFS) cleanUpPersistentDirCache(cacheErr error) error {
	if vfs.dirDB == nil {
		return cacheErr
	}
	return errors.Join(cacheErr, vfs.dirDB.Purge())
}

func (d *Dir) invalidatePersistentDirectory(dirPath string) {
	if d.vfs.dirDB == nil {
		return
	}
	if err := d.vfs.dirDB.InvalidateDirectory(dirPath); err != nil {
		fs.Errorf(dirPath, "Failed to invalidate persistent VFS directory cache: %v", err)
	}
}

func (d *Dir) invalidatePersistentSubtree(dirPath string) {
	if d.vfs.dirDB == nil {
		return
	}
	if err := d.vfs.dirDB.InvalidateSubtree(dirPath); err != nil {
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

// restorePersistentDirLocked restores d after an in-memory cache miss.
// d.mu must be held.
func (d *Dir) restorePersistentDirLocked() bool {
	if d.vfs.dirDB == nil {
		return false
	}
	entries, refreshedAt, found, err := d.vfs.dirDB.LoadDirectory(
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
	if d.vfs.dirDB == nil {
		return
	}
	if len(d.virtual) != 0 {
		fs.Debugf(d.path, "Not saving persistent VFS directory cache while virtual entries are pending")
		return
	}
	if err := d.vfs.dirDB.SaveDirectory(d.vfs.ctx, d.path, entries, d.read); err != nil {
		fs.Errorf(d.path, "Failed to save persistent VFS directory cache: %v", err)
	}
}

func (d *Dir) persistentTreeMutation() uint64 {
	if d.vfs.dirDB == nil {
		return 0
	}
	return d.vfs.dirDB.MutationVersion()
}

func (d *Dir) replacePersistentTree(dirPath string, tree dirtree.DirTree, refreshedAt time.Time, mutation uint64) error {
	if d.vfs.dirDB == nil {
		return nil
	}
	if d.hasVirtual() {
		return errors.New("can't save persistent VFS directory tree while virtual entries are pending")
	}
	if err := d.vfs.dirDB.ReplaceTree(d.vfs.ctx, dirPath, tree, refreshedAt, mutation); err != nil {
		return fmt.Errorf("failed to save persistent VFS directory tree: %w", err)
	}
	return nil
}
