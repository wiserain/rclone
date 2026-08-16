package fs

import "context"

// PersistentDirCachePolicy controls optional metadata stored by a backend codec.
type PersistentDirCachePolicy struct {
	NoChecksum bool // omit checksums when VFS checksum verification is disabled
	NoModTime  bool // omit file modification times when VFS modtimes are disabled
}

// PersistentDirCacheCodec is an optional interface which allows a backend to
// persist and restore its concrete directory entries without an additional
// metadata request after the VFS directory cache is restored from disk.
//
// EncodePersistentDirEntry must return enough backend-private data to restore
// a concrete entry. The returned byte slice is opaque to the VFS and is passed
// back unchanged to DecodePersistentDirEntry.
//
// DecodePersistentDirEntry is only called when non-empty backend data was
// stored. It must return either an Object or Directory matching isDir.
type PersistentDirCacheCodec interface {
	EncodePersistentDirEntry(ctx context.Context, entry DirEntry, policy PersistentDirCachePolicy) ([]byte, error)
	DecodePersistentDirEntry(ctx context.Context, remote string, isDir bool, data []byte) (DirEntry, error)
}

// PersistentDirCacher is implemented by backends which can safely persist and
// restore concrete directory entries for the same storage identity.
type PersistentDirCacher interface {
	PersistentDirCacheCodec
	PersistentDirCacheIdentityer
}

// GetPersistentDirCacher returns the persistent directory cache capability of f.
func GetPersistentDirCacher(f Fs) (PersistentDirCacher, bool) {
	persistent, ok := f.(PersistentDirCacher)
	return persistent, ok
}

// PersistentDirCacheIdentityer optionally supplies a stable backend/account
// identity for the persistent VFS directory cache. It should exclude rotating
// credentials such as access tokens or refreshed cookies.
type PersistentDirCacheIdentityer interface {
	PersistentDirCacheIdentity() string
}
