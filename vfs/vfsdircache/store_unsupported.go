//go:build plan9 || js

// Package vfsdircache persists VFS directory listings under --cache-dir.
package vfsdircache

import (
	"context"
	"errors"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/vfs/vfscommon"
)

var errUnsupported = errors.New("persistent VFS directory cache is not supported on this platform")

// Store is unavailable on platforms unsupported by bbolt.
type Store struct{}

// New reports that the persistent directory cache is unsupported.
func New(context.Context, fs.Fs, *vfscommon.Options) (*Store, error) {
	return nil, errUnsupported
}

// Close closes the persistent cache database.
func (*Store) Close() error { return nil }

// MutationVersion returns the current invalidation version.
func (*Store) MutationVersion() uint64 { return 0 }

// Path returns the database path.
func (*Store) Path() string { return "UNSUPPORTED" }

// LoadDirectory reports a cache miss.
func (*Store) LoadDirectory(context.Context, string, time.Duration) (fs.DirEntries, time.Time, bool, error) {
	return nil, time.Time{}, false, errUnsupported
}

// SaveDirectory reports that persistence is unsupported.
func (*Store) SaveDirectory(context.Context, string, fs.DirEntries, time.Time) error {
	return errUnsupported
}

// ReplaceTree reports that persistence is unsupported.
func (*Store) ReplaceTree(context.Context, string, dirtree.DirTree, time.Time, uint64) error {
	return errUnsupported
}

// InvalidateDirectory reports that persistence is unsupported.
func (*Store) InvalidateDirectory(string) error { return errUnsupported }

// InvalidateSubtree reports that persistence is unsupported.
func (*Store) InvalidateSubtree(string) error { return errUnsupported }

// Stats returns an unsupported status.
func (*Store) Stats() rc.Params {
	return rc.Params{"open": false, "supported": false}
}
