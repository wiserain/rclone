package fs

import "context"

// PersistentDirCacheCodec is an optional interface which allows a backend to
// persist and restore its concrete directory entries without an additional
// metadata request after the VFS directory cache is restored from disk.
//
// EncodePersistentDirEntry should return nil, nil when the backend does not
// need any private data for entry. The returned byte slice is opaque to the
// VFS and is passed back unchanged to DecodePersistentDirEntry.
//
// DecodePersistentDirEntry is only called when non-empty backend data was
// stored. It must return either an Object or Directory matching isDir.
type PersistentDirCacheCodec interface {
	EncodePersistentDirEntry(ctx context.Context, entry DirEntry) ([]byte, error)
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
