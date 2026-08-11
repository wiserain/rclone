//go:build !plan9 && !js

package vfsdircache

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

// persistentObject is a generic object restored from the on-disk directory
// cache. Metadata used by directory listings is served from the saved record;
// operations which require the concrete backend object resolve it lazily.
type persistentObject struct {
	f        fs.Fs
	remote   string
	size     int64
	modTime  time.Time
	storable bool
	id       string
	parentID string

	mu   sync.Mutex
	live fs.Object
}

func newPersistentObject(f fs.Fs, entry entryRecord) fs.Object {
	return &persistentObject{
		f:        f,
		remote:   entry.Remote,
		size:     entry.Size,
		modTime:  decodeTime(entry.ModTimeUnixNano, entry.ModTimeValid),
		storable: entry.Storable,
		id:       entry.ID,
		parentID: entry.ParentID,
	}
}

func (o *persistentObject) Fs() fs.Info {
	return o.f
}

func (o *persistentObject) String() string {
	return o.remote
}

func (o *persistentObject) Remote() string {
	return o.remote
}

func (o *persistentObject) ModTime(ctx context.Context) time.Time {
	o.mu.Lock()
	live := o.live
	modTime := o.modTime
	o.mu.Unlock()
	if live != nil {
		return live.ModTime(ctx)
	}
	return modTime
}

func (o *persistentObject) Size() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.live != nil {
		return o.live.Size()
	}
	return o.size
}

func (o *persistentObject) Storable() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.live != nil {
		return o.live.Storable()
	}
	return o.storable
}

func (o *persistentObject) Hash(ctx context.Context, ty hash.Type) (string, error) {
	live, err := o.ResolvePersistentObject(ctx)
	if err != nil {
		return "", err
	}
	return live.Hash(ctx, ty)
}

func (o *persistentObject) SetModTime(ctx context.Context, t time.Time) error {
	live, err := o.ResolvePersistentObject(ctx)
	if err != nil {
		return err
	}
	if err = live.SetModTime(ctx, t); err != nil {
		return err
	}
	o.mu.Lock()
	o.modTime = t
	o.mu.Unlock()
	return nil
}

func (o *persistentObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	live, err := o.ResolvePersistentObject(ctx)
	if err != nil {
		return nil, err
	}
	return live.Open(ctx, options...)
}

func (o *persistentObject) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	live, err := o.ResolvePersistentObject(ctx)
	if err != nil {
		return err
	}
	if err = live.Update(ctx, in, src, options...); err != nil {
		return err
	}
	o.mu.Lock()
	o.size = live.Size()
	o.modTime = live.ModTime(ctx)
	o.storable = live.Storable()
	o.mu.Unlock()
	return nil
}

func (o *persistentObject) Remove(ctx context.Context) error {
	live, err := o.ResolvePersistentObject(ctx)
	if err != nil {
		return err
	}
	return live.Remove(ctx)
}

// ResolvePersistentObject resolves the concrete backend object once and caches
// it for all subsequent operations.
func (o *persistentObject) ResolvePersistentObject(ctx context.Context) (fs.Object, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.live != nil {
		return o.live, nil
	}
	live, err := o.f.NewObject(ctx, o.remote)
	if err != nil {
		return nil, err
	}
	o.live = live
	o.size = live.Size()
	o.modTime = live.ModTime(ctx)
	o.storable = live.Storable()
	if ider, ok := live.(fs.IDer); ok {
		o.id = ider.ID()
	}
	if parentIDer, ok := live.(fs.ParentIDer); ok {
		o.parentID = parentIDer.ParentID()
	}
	return live, nil
}

// ID returns the stored object ID when available.
func (o *persistentObject) ID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ider, ok := o.live.(fs.IDer); ok {
		return ider.ID()
	}
	return o.id
}

// ParentID returns the stored parent ID when available.
func (o *persistentObject) ParentID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if parentIDer, ok := o.live.(fs.ParentIDer); ok {
		return parentIDer.ParentID()
	}
	return o.parentID
}

// UnWrap returns the concrete object after it has been resolved. It deliberately
// returns nil before that point so callers do not trigger network I/O merely by
// checking optional interfaces.
func (o *persistentObject) UnWrap() fs.Object {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.live
}

var (
	_ fs.Object                   = (*persistentObject)(nil)
	_ fs.IDer                     = (*persistentObject)(nil)
	_ fs.ParentIDer               = (*persistentObject)(nil)
	_ fs.ObjectUnWrapper          = (*persistentObject)(nil)
	_ fs.PersistentObjectResolver = (*persistentObject)(nil)
)
