package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/rclone/rclone/fs"
)

const persistentDirCacheRecordVersion = 1

const (
	persistentDriveDirectory uint8 = iota
	persistentDriveObject
	persistentDriveDocument
	persistentDriveLink
)

type persistentDriveRecord struct {
	Version          int         `json:"v"`
	Kind             uint8       `json:"k,omitempty"`
	ID               string      `json:"i"`
	ModifiedDate     string      `json:"d,omitempty"`
	MimeType         string      `json:"m,omitempty"`
	Bytes            int64       `json:"s"`
	Parents          []string    `json:"p,omitempty"`
	ResourceKey      string      `json:"r,omitempty"`
	Metadata         fs.Metadata `json:"x,omitempty"`
	MD5Sum           string      `json:"h5,omitempty"`
	SHA1Sum          string      `json:"h1,omitempty"`
	SHA256Sum        string      `json:"h256,omitempty"`
	V2Download       bool        `json:"v2,omitempty"`
	URL              string      `json:"u,omitempty"`
	DocumentMimeType string      `json:"dm,omitempty"`
	ExtensionLength  int         `json:"e,omitempty"`
	Content          []byte      `json:"c,omitempty"`
}

type persistentDriveIdentity struct {
	RootFolderID          string `json:"root_folder_id"`
	TeamDriveID           string `json:"team_drive_id,omitempty"`
	IsTeamDrive           bool   `json:"is_team_drive"`
	Scope                 string `json:"scope"`
	AuthOwnerOnly         bool   `json:"auth_owner_only"`
	CopyShortcutContent   bool   `json:"copy_shortcut_content"`
	SkipGdocs             bool   `json:"skip_gdocs"`
	ShowAllGdocs          bool   `json:"show_all_gdocs"`
	SkipChecksumGphotos   bool   `json:"skip_checksum_gphotos"`
	SharedWithMe          bool   `json:"shared_with_me"`
	TrashedOnly           bool   `json:"trashed_only"`
	StarredOnly           bool   `json:"starred_only"`
	Extensions            string `json:"formats"`
	ExportExtensions      string `json:"export_formats"`
	UseCreatedDate        bool   `json:"use_created_date"`
	UseSharedDate         bool   `json:"use_shared_date"`
	SizeAsQuota           bool   `json:"size_as_quota"`
	V2DownloadMinSize     int64  `json:"v2_download_min_size"`
	SkipShortcuts         bool   `json:"skip_shortcuts"`
	SkipDanglingShortcuts bool   `json:"skip_dangling_shortcuts"`
	Encoding              string `json:"encoding"`
	Metadata              bool   `json:"metadata"`
}

// PersistentDirCacheIdentity implements fs.PersistentDirCacheIdentityer.
func (f *Fs) PersistentDirCacheIdentity() string {
	identity := persistentDriveIdentity{
		RootFolderID:          f.rootFolderID,
		TeamDriveID:           f.opt.TeamDriveID,
		IsTeamDrive:           f.isTeamDrive,
		Scope:                 f.opt.Scope,
		AuthOwnerOnly:         f.opt.AuthOwnerOnly,
		CopyShortcutContent:   f.opt.CopyShortcutContent,
		SkipGdocs:             f.opt.SkipGdocs,
		ShowAllGdocs:          f.opt.ShowAllGdocs,
		SkipChecksumGphotos:   f.opt.SkipChecksumGphotos,
		SharedWithMe:          f.opt.SharedWithMe,
		TrashedOnly:           f.opt.TrashedOnly,
		StarredOnly:           f.opt.StarredOnly,
		Extensions:            f.opt.Extensions,
		ExportExtensions:      f.opt.ExportExtensions,
		UseCreatedDate:        f.opt.UseCreatedDate,
		UseSharedDate:         f.opt.UseSharedDate,
		SizeAsQuota:           f.opt.SizeAsQuota,
		V2DownloadMinSize:     int64(f.opt.V2DownloadMinSize),
		SkipShortcuts:         f.opt.SkipShortcuts,
		SkipDanglingShortcuts: f.opt.SkipDanglingShortcuts,
		Encoding:              f.opt.Enc.String(),
	}
	if f.ci != nil {
		identity.Metadata = f.ci.Metadata
	}
	data, err := json.Marshal(identity)
	if err != nil {
		panic(fmt.Sprintf("failed to encode Google Drive persistent directory cache identity: %v", err))
	}
	return string(data)
}

func persistentDriveBaseRecord(base *baseObject) persistentDriveRecord {
	record := persistentDriveRecord{
		Version:      persistentDirCacheRecordVersion,
		ID:           base.id,
		ModifiedDate: base.modifiedDate,
		MimeType:     base.mimeType,
		Bytes:        base.bytes,
		Parents:      slices.Clone(base.parents),
	}
	if base.resourceKey != nil {
		record.ResourceKey = *base.resourceKey
	}
	if base.metadata != nil {
		record.Metadata = maps.Clone(*base.metadata)
	}
	return record
}

// PersistentDirCacheCodecVersion returns the persistent entry record version.
func (f *Fs) PersistentDirCacheCodecVersion() int {
	return persistentDirCacheRecordVersion
}

// EncodePersistentDirEntry implements fs.PersistentDirCacheCodec.
func (f *Fs) EncodePersistentDirEntry(_ context.Context, entry fs.DirEntry) ([]byte, error) {
	var record persistentDriveRecord
	switch item := entry.(type) {
	case *Object:
		record = persistentDriveBaseRecord(&item.baseObject)
		record.Kind = persistentDriveObject
		record.MD5Sum = item.md5sum
		record.SHA1Sum = item.sha1sum
		record.SHA256Sum = item.sha256sum
		record.V2Download = item.v2Download
	case *documentObject:
		record = persistentDriveBaseRecord(&item.baseObject)
		record.Kind = persistentDriveDocument
		record.URL = item.url
		record.DocumentMimeType = item.documentMimeType
		record.ExtensionLength = item.extLen
	case *linkObject:
		record = persistentDriveBaseRecord(&item.baseObject)
		record.Kind = persistentDriveLink
		record.Content = slices.Clone(item.content)
		record.ExtensionLength = item.extLen
	case *Directory:
		record = persistentDriveBaseRecord(&item.baseObject)
		record.Kind = persistentDriveDirectory
	default:
		return nil, fmt.Errorf("can't persist unsupported Google Drive directory entry type %T", entry)
	}
	return json.Marshal(record)
}

func (f *Fs) restorePersistentDriveBase(remote string, record *persistentDriveRecord) baseObject {
	base := baseObject{
		fs:           f,
		remote:       remote,
		id:           record.ID,
		modifiedDate: record.ModifiedDate,
		mimeType:     record.MimeType,
		bytes:        record.Bytes,
		parents:      slices.Clone(record.Parents),
	}
	if record.ResourceKey != "" {
		base.resourceKey = &record.ResourceKey
	}
	if record.Metadata != nil {
		metadata := maps.Clone(record.Metadata)
		base.metadata = &metadata
	}
	return base
}

// DecodePersistentDirEntry implements fs.PersistentDirCacheCodec.
func (f *Fs) DecodePersistentDirEntry(_ context.Context, remote string, isDir bool, data []byte) (fs.DirEntry, error) {
	var record persistentDriveRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to decode Google Drive persistent directory cache record: %w", err)
	}
	if record.Version != persistentDirCacheRecordVersion {
		return nil, fmt.Errorf("unsupported Google Drive persistent directory cache record version %d", record.Version)
	}
	if record.ID == "" {
		return nil, fmt.Errorf("google drive persistent directory cache record for %q has no ID", remote)
	}
	base := f.restorePersistentDriveBase(remote, &record)
	switch record.Kind {
	case persistentDriveDirectory:
		if !isDir {
			return nil, fmt.Errorf("google drive persistent directory cache kind mismatch for %q", remote)
		}
		f.dirCache.Put(remote, record.ID)
		if record.ResourceKey != "" {
			f.dirResourceKeys.Store(record.ID, record.ResourceKey)
		}
		return &Directory{baseObject: base}, nil
	case persistentDriveObject:
		if isDir {
			return nil, fmt.Errorf("google drive persistent object cache kind mismatch for %q", remote)
		}
		return &Object{
			baseObject: base,
			url:        fmt.Sprintf("%sfiles/%s?alt=media", f.svc.BasePath, actualID(record.ID)),
			md5sum:     record.MD5Sum,
			sha1sum:    record.SHA1Sum,
			sha256sum:  record.SHA256Sum,
			v2Download: record.V2Download,
		}, nil
	case persistentDriveDocument:
		if isDir {
			return nil, fmt.Errorf("google drive persistent document cache kind mismatch for %q", remote)
		}
		return &documentObject{
			baseObject:       base,
			url:              record.URL,
			documentMimeType: record.DocumentMimeType,
			extLen:           record.ExtensionLength,
		}, nil
	case persistentDriveLink:
		if isDir {
			return nil, fmt.Errorf("google drive persistent link cache kind mismatch for %q", remote)
		}
		return &linkObject{
			baseObject: base,
			content:    slices.Clone(record.Content),
			extLen:     record.ExtensionLength,
		}, nil
	default:
		return nil, fmt.Errorf("unknown Google Drive persistent directory cache kind %d for %q", record.Kind, remote)
	}
}

var (
	_ fs.PersistentDirCacheCodec      = (*Fs)(nil)
	_ fs.PersistentDirCacheIdentityer = (*Fs)(nil)
)
