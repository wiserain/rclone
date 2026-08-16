package _115 // nolint:revive

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
)

const persistentDirCacheRecordVersion = 1

const (
	persistent115Directory uint8 = iota
	persistent115Object
)

// persistentDirCacheRecord contains the backend-private values needed to
// reconstruct a concrete 115 object or directory without another metadata API
// call after the VFS directory cache has been restored from disk.
type persistentDirCacheRecord struct {
	Version          int    `json:"v"`
	Kind             uint8  `json:"k,omitempty"`
	ID               string `json:"i"`
	ParentID         string `json:"p"`
	Size             int64  `json:"s"`
	Items            int64  `json:"n,omitempty"`
	SHA1             string `json:"h,omitempty"`
	PickCode         string `json:"c,omitempty"`
	ModTimeUnixNano  int64  `json:"t,omitempty"`
	ModTimeIsPresent bool   `json:"q,omitempty"`
}

// PersistentDirCacheIdentity implements fs.PersistentDirCacheIdentityer.
// userID stays stable when the user rotates cookies, but changes when the
// configured remote is pointed at a different 115 account.
func (f *Fs) PersistentDirCacheIdentity() string {
	return fmt.Sprintf(
		"user_id=%s;root_folder_id=%s;share=%t;share_code=%s",
		f.userID,
		f.rootFolderID,
		f.isShare,
		f.opt.ShareCode,
	)
}

// EncodePersistentDirEntry implements fs.PersistentDirCacheCodec.
//
// Directory IDs are saved as well as object IDs, SHA1 and pickcode. Restoring
// directory IDs also repopulates the backend's own path-to-ID cache, which
// avoids a path traversal API call when a later operation needs that directory.
func (f *Fs) EncodePersistentDirEntry(ctx context.Context, entry fs.DirEntry) ([]byte, error) {
	record := persistentDirCacheRecord{
		Version: persistentDirCacheRecordVersion,
	}
	modTime := entry.ModTime(ctx)
	if !modTime.IsZero() {
		record.ModTimeUnixNano = modTime.UnixNano()
		record.ModTimeIsPresent = true
	}

	switch item := entry.(type) {
	case *Object:
		if err := item.readMetaData(ctx); err != nil {
			return nil, err
		}
		record.Kind = persistent115Object
		record.ID = item.id
		record.ParentID = item.parent
		record.Size = item.size
		record.SHA1 = item.sha1sum
		record.PickCode = item.pickCode
		record.ModTimeUnixNano = item.modTime.UnixNano()
		record.ModTimeIsPresent = !item.modTime.IsZero()
	case fs.Directory:
		record.Kind = persistent115Directory
		record.ID = item.ID()
		record.Size = item.Size()
		record.Items = item.Items()
		if parentIDer, ok := item.(fs.ParentIDer); ok {
			record.ParentID = parentIDer.ParentID()
		}
	default:
		return nil, fmt.Errorf("can't persist unsupported 115 directory entry type %T", entry)
	}

	return json.Marshal(record)
}

// DecodePersistentDirEntry implements fs.PersistentDirCacheCodec.
func (f *Fs) DecodePersistentDirEntry(_ context.Context, remote string, isDir bool, data []byte) (fs.DirEntry, error) {
	var record persistentDirCacheRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to decode 115 persistent directory cache record: %w", err)
	}
	if record.Version != persistentDirCacheRecordVersion {
		return nil, fmt.Errorf("unsupported 115 persistent directory cache record version %d", record.Version)
	}

	modTime := time.Time{}
	if record.ModTimeIsPresent {
		modTime = time.Unix(0, record.ModTimeUnixNano)
	}

	if isDir {
		if record.Kind != persistent115Directory {
			return nil, fmt.Errorf("115 persistent directory cache kind mismatch for %q: %d", remote, record.Kind)
		}
		if record.ID == "" {
			return nil, fmt.Errorf("115 persistent directory cache record for %q has no directory ID", remote)
		}
		f.dirCache.Put(remote, record.ID)
		return fs.NewDir(remote, modTime).
			SetSize(record.Size).
			SetItems(record.Items).
			SetID(record.ID).
			SetParentID(record.ParentID), nil
	}

	if record.Kind != persistent115Object {
		return nil, fmt.Errorf("115 persistent object cache kind mismatch for %q: %d", remote, record.Kind)
	}
	if record.ID == "" {
		return nil, fmt.Errorf("115 persistent object cache record for %q has no file ID", remote)
	}
	if !f.isShare && record.Size != 0 && record.PickCode == "" {
		return nil, fmt.Errorf("115 persistent object cache record for %q has no pickcode", remote)
	}
	return &Object{
		fs:          f,
		remote:      remote,
		hasMetaData: true,
		id:          record.ID,
		parent:      record.ParentID,
		size:        record.Size,
		sha1sum:     strings.ToLower(record.SHA1),
		pickCode:    record.PickCode,
		modTime:     modTime,
		durlMu:      new(sync.Mutex),
	}, nil
}

var (
	_ fs.PersistentDirCacheCodec      = (*Fs)(nil)
	_ fs.PersistentDirCacheIdentityer = (*Fs)(nil)
)
