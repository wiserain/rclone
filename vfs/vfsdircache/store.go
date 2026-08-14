//go:build !plan9 && !js

// Package vfsdircache persists VFS directory listings under --cache-dir.
package vfsdircache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/file"
	"github.com/rclone/rclone/vfs/vfscommon"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

const (
	databaseSchema  = 1
	recordSchema    = 1
	databaseName    = "dircache.db"
	cacheRootName   = "vfsDirCache"
	writeBatchSize  = 256
	writeBatchBytes = 16 * 1024 * 1024

	entryObject uint8 = 1
	entryDir    uint8 = 2
)

var (
	bucketMeta = []byte("meta")
	bucketDirs = []byte("dirs")

	keySchema       = []byte("schema")
	keyIdentity     = []byte("identity")
	keySnapshotTime = []byte("snapshot_time")
)

var errIncompatibleDatabase = errors.New("incompatible persistent VFS directory cache")

var (
	errStoreClosed              = errors.New("persistent VFS directory cache is closed")
	errChangedDuringTreeRefresh = errors.New("persistent VFS directory cache changed during recursive refresh")
)

// Store is an on-disk VFS directory listing cache.
type Store struct {
	f        fs.Fs
	codec    fs.PersistentDirCacheCodec
	root     string
	path     string
	identity string

	mu     sync.RWMutex
	db     *bolt.DB
	closed bool

	hits      atomic.Uint64
	misses    atomic.Uint64
	expired   atomic.Uint64
	errors    atomic.Uint64
	writes    atomic.Uint64
	mutations atomic.Uint64
}

type directoryRecord struct {
	Schema              uint16
	Path                string
	RefreshedAtUnixNano int64
	Entries             []entryRecord
}

type entryRecord struct {
	Kind            uint8
	Remote          string
	ModTimeUnixNano int64
	ModTimeValid    bool
	Size            int64
	Items           int64
	ID              string
	ParentID        string
	Storable        bool
	BackendData     []byte
}

type encodedDirectory struct {
	path string
	data []byte
}

// New opens or creates the persistent VFS directory cache for f.
func New(ctx context.Context, f fs.Fs, opt *vfscommon.Options) (*Store, error) {
	persistent, ok := fs.GetPersistentDirCacher(f)
	if !ok {
		return nil, fmt.Errorf("backend %q does not support persistent VFS directory caching", f.Name())
	}
	root, dbPath, err := cachePaths(f)
	if err != nil {
		return nil, err
	}
	if err = file.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("failed to create persistent VFS directory cache root: %w", err)
	}
	if err = recoverInterruptedSwap(dbPath); err != nil {
		return nil, err
	}

	s := &Store{
		f:        f,
		codec:    persistent,
		root:     root,
		path:     dbPath,
		identity: makeIdentity(ctx, f, opt, persistent.PersistentDirCacheIdentity()),
	}
	if err = s.openCurrent(); err == nil {
		// A previous process may have stopped after installing and validating a
		// new database but before deleting its backup.
		_ = os.Remove(s.path + ".old")
		removeStaleTemporaryFiles(s.path)
		return s, nil
	}
	if !errors.Is(err, errIncompatibleDatabase) {
		return nil, err
	}

	// Preserve the latest incompatible cache for diagnosis, then start clean.
	stalePath := s.path + ".incompatible"
	if removeErr := os.Remove(stalePath); removeErr != nil && !os.IsNotExist(removeErr) {
		return nil, fmt.Errorf("failed to remove previous incompatible persistent VFS directory cache: %w", removeErr)
	}
	if renameErr := os.Rename(s.path, stalePath); renameErr != nil && !os.IsNotExist(renameErr) {
		return nil, fmt.Errorf("failed to move incompatible persistent VFS directory cache aside: %w", renameErr)
	}
	if err = s.openCurrent(); err != nil {
		return nil, err
	}
	removeStaleTemporaryFiles(s.path)
	return s, nil
}

func removeStaleTemporaryFiles(dbPath string) {
	matches, err := filepath.Glob(dbPath + ".tmp-*")
	if err != nil {
		return
	}
	for _, match := range matches {
		_ = os.Remove(match)
	}
}

func recoverInterruptedSwap(dbPath string) error {
	if _, err := os.Stat(dbPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to inspect persistent VFS directory cache %q: %w", dbPath, err)
	}

	backupPath := dbPath + ".old"
	if _, err := os.Stat(backupPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to inspect persistent VFS directory cache backup %q: %w", backupPath, err)
	}
	if err := os.Rename(backupPath, dbPath); err != nil {
		return fmt.Errorf("failed to recover interrupted persistent VFS directory cache swap: %w", err)
	}
	return nil
}

func cachePaths(f fs.Fs) (root, dbPath string, err error) {
	parentOSPath := config.GetCacheDir()
	relativeDirPath := f.Root()
	if runtime.GOOS == "windows" && strings.HasPrefix(relativeDirPath, `//?/`) {
		relativeDirPath = relativeDirPath[2:]
	}
	relativeDirPath = cleanRemotePath(f.Name() + "/" + relativeDirPath)
	relativeDirOSPath := filepath.FromSlash(encoder.OS.FromStandardPath(relativeDirPath))
	root = file.UNCPath(filepath.Join(parentOSPath, cacheRootName, relativeDirOSPath))
	dbPath = filepath.Join(root, databaseName)
	return root, dbPath, nil
}

func cleanRemotePath(name string) string {
	name = strings.Trim(name, "/")
	name = path.Clean(name)
	if name == "." || name == "/" {
		return ""
	}
	return name
}

func makeIdentity(ctx context.Context, f fs.Fs, opt *vfscommon.Options, backendIdentity string) string {
	ci := fs.GetConfig(ctx)
	filterConfig := filter.GetConfig(ctx)

	hasher := sha256.New()
	_, _ = fmt.Fprintf(
		hasher,
		"schema=%d\nfs=%s\nname=%s\nroot=%s\nbackend_identity=%q\nfilter_options=%#v\nfilter_rules=%s\nfilter_mod_time_from=%d\nfilter_mod_time_to=%d\nlinks=%t\ncase_insensitive=%t\nblock_norm_dupes=%t\nmetadata_extension=%q\nno_unicode_normalization=%t\nignore_case_sync=%t\n",
		databaseSchema,
		fs.ConfigString(f),
		f.Name(),
		f.Root(),
		backendIdentity,
		filterConfig.Opt,
		filterConfig.DumpFilters(),
		filterConfig.ModTimeFrom.UnixNano(),
		filterConfig.ModTimeTo.UnixNano(),
		opt.Links,
		opt.CaseInsensitive,
		opt.BlockNormDupes,
		opt.MetadataExtension,
		ci.NoUnicodeNormalization,
		ci.IgnoreCaseSync,
	)

	// --files-from contents affect the visible tree but are not fully represented
	// by DumpFilters. Sort them before hashing so map iteration cannot make the
	// identity change nondeterministically between starts.
	files := filterConfig.Files()
	fileNames := make([]string, 0, len(files))
	for name := range files {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)
	for _, name := range fileNames {
		_, _ = fmt.Fprintf(hasher, "files_from_entry=%q\n", name)
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func openDatabase(dbPath string) (*bolt.DB, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open persistent VFS directory cache %q: %w", dbPath, err)
	}
	return db, nil
}

func initializeDatabase(db *bolt.DB, identity string) error {
	return db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketDirs); err != nil {
			return err
		}

		schemaValue := meta.Get(keySchema)
		identityValue := meta.Get(keyIdentity)
		if schemaValue == nil && identityValue == nil {
			if err = meta.Put(keySchema, []byte(strconv.Itoa(databaseSchema))); err != nil {
				return err
			}
			return meta.Put(keyIdentity, []byte(identity))
		}
		if string(schemaValue) != strconv.Itoa(databaseSchema) || string(identityValue) != identity {
			return errIncompatibleDatabase
		}
		return nil
	})
}

func (s *Store) openCurrent() error {
	db, err := openDatabase(s.path)
	if err != nil {
		return err
	}
	if err = initializeDatabase(db, s.identity); err != nil {
		_ = db.Close()
		return err
	}
	s.db = db
	return nil
}

// Close closes the persistent cache database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// MutationVersion returns a process-local version which changes whenever a
// directory record is written, invalidated, or replaced. A recursive refresh
// captures this before walking the remote so that it cannot later overwrite a
// newer directory read or local VFS change which happened during a long
// traversal.
func (s *Store) MutationVersion() uint64 {
	return s.mutations.Load()
}

// Path returns the database path.
func (s *Store) Path() string {
	return s.path
}

func (s *Store) checkOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.db == nil {
		return errStoreClosed
	}
	return nil
}

func (s *Store) checkRefreshVersion(expectedMutation uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.db == nil {
		return errStoreClosed
	}
	if s.mutations.Load() != expectedMutation {
		return errChangedDuringTreeRefresh
	}
	return nil
}

// LoadDirectory loads dir from disk when it is present and not older than
// maxAge. A found value of false means the caller should read the remote.
func (s *Store) LoadDirectory(ctx context.Context, dir string, maxAge time.Duration) (entries fs.DirEntries, refreshedAt time.Time, found bool, err error) {
	var data []byte
	s.mu.RLock()
	if s.db == nil {
		closed := s.closed
		s.mu.RUnlock()
		if closed {
			return nil, time.Time{}, false, errStoreClosed
		}
		s.misses.Add(1)
		return nil, time.Time{}, false, nil
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketDirs)
		if bucket == nil {
			return nil
		}
		value := bucket.Get(directoryKey(dir))
		if value != nil {
			data = append(data, value...)
		}
		return nil
	})
	s.mu.RUnlock()
	if err != nil {
		s.errors.Add(1)
		return nil, time.Time{}, false, fmt.Errorf("failed to read persistent directory %q: %w", dir, err)
	}
	if data == nil {
		s.misses.Add(1)
		return nil, time.Time{}, false, nil
	}

	record, err := decodeDirectoryRecord(data)
	if err != nil {
		s.errors.Add(1)
		return nil, time.Time{}, false, fmt.Errorf("failed to decode persistent directory %q: %w", dir, err)
	}
	if record.Path != cleanRemotePath(dir) {
		s.errors.Add(1)
		return nil, time.Time{}, false, fmt.Errorf("persistent directory key mismatch: wanted %q, got %q", dir, record.Path)
	}
	refreshedAt = time.Unix(0, record.RefreshedAtUnixNano)
	if time.Since(refreshedAt) > maxAge {
		s.expired.Add(1)
		return nil, refreshedAt, false, nil
	}
	entries, err = s.restoreEntries(ctx, record.Entries)
	if err != nil {
		s.errors.Add(1)
		return nil, time.Time{}, false, fmt.Errorf("failed to restore persistent directory %q: %w", dir, err)
	}
	s.hits.Add(1)
	return entries, refreshedAt, true, nil
}

// SaveDirectory writes one freshly read remote directory to disk.
func (s *Store) SaveDirectory(ctx context.Context, dir string, entries fs.DirEntries, refreshedAt time.Time) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	data, err := s.encodeDirectory(ctx, dir, entries, refreshedAt)
	if err != nil {
		s.errors.Add(1)
		return err
	}
	newRecord, err := decodeDirectoryRecord(data)
	if err != nil {
		s.errors.Add(1)
		return fmt.Errorf("failed to verify encoded persistent directory %q: %w", dir, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return errStoreClosed
	}
	if err = s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketDirs)
		key := directoryKey(dir)
		if oldData := bucket.Get(key); oldData != nil {
			oldRecord, decodeErr := decodeDirectoryRecord(oldData)
			if decodeErr != nil || oldRecord.Path != newRecord.Path {
				// Without a valid parent record it isn't possible to decide which
				// descendants still belong to the current directory.
				if err := deleteDescendants(bucket, dir); err != nil {
					return err
				}
			} else {
				if _, err := deleteReplacedChildSubtrees(bucket, oldRecord, newRecord); err != nil {
					return err
				}
			}
		}
		return bucket.Put(key, data)
	}); err != nil {
		s.errors.Add(1)
		return fmt.Errorf("failed to save persistent directory %q: %w", dir, err)
	}
	s.writes.Add(1)
	// Any independently refreshed directory may be newer than a recursive
	// walk which started earlier, even when its child directory structure did
	// not change. Prevent that older walk from replacing this record.
	s.mutations.Add(1)
	return nil
}

// ReplaceTree saves a recursively refreshed tree. A root refresh is built in a
// new database and swapped into place only after it is complete. A subtree
// refresh is replaced atomically in the current database.
func (s *Store) ReplaceTree(ctx context.Context, root string, tree dirtree.DirTree, refreshedAt time.Time, expectedMutation uint64) error {
	if err := s.checkRefreshVersion(expectedMutation); err != nil {
		return err
	}
	root = cleanRemotePath(root)
	if root == "" {
		return s.replaceAll(ctx, tree, refreshedAt, expectedMutation)
	}
	return s.replaceSubtree(ctx, root, tree, refreshedAt, expectedMutation)
}

func (s *Store) replaceAll(ctx context.Context, tree dirtree.DirTree, refreshedAt time.Time, expectedMutation uint64) (err error) {
	tmpPath := fmt.Sprintf("%s.tmp-%d-%d", s.path, os.Getpid(), time.Now().UnixNano())
	defer func() {
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	tmpDB, err := openDatabase(tmpPath)
	if err != nil {
		return err
	}
	closeTmp := true
	defer func() {
		if closeTmp {
			_ = tmpDB.Close()
		}
	}()
	if err = initializeDatabase(tmpDB, s.identity); err != nil {
		return err
	}

	paths := sortedTreePaths(tree)
	batch := make([]encodedDirectory, 0, writeBatchSize)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err = tmpDB.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(bucketDirs)
			for _, item := range batch {
				if putErr := bucket.Put(directoryKey(item.path), item.data); putErr != nil {
					return putErr
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("failed writing persistent directory snapshot: %w", err)
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	for _, dir := range paths {
		data, encodeErr := s.encodeDirectory(ctx, dir, tree[dir], refreshedAt)
		if encodeErr != nil {
			return encodeErr
		}
		recordBytes := len(dir) + len(data)
		if len(batch) != 0 && (len(batch) >= writeBatchSize || batchBytes+recordBytes > writeBatchBytes) {
			if err = flush(); err != nil {
				return err
			}
		}
		batch = append(batch, encodedDirectory{path: dir, data: data})
		batchBytes += recordBytes
		if len(batch) >= writeBatchSize || batchBytes >= writeBatchBytes {
			if err = flush(); err != nil {
				return err
			}
		}
	}
	if err = flush(); err != nil {
		return err
	}
	if err = tmpDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(keySnapshotTime, []byte(strconv.FormatInt(refreshedAt.UnixNano(), 10)))
	}); err != nil {
		return fmt.Errorf("failed finalizing persistent directory snapshot: %w", err)
	}
	if err = tmpDB.Sync(); err != nil {
		return fmt.Errorf("failed syncing persistent directory snapshot: %w", err)
	}
	if err = tmpDB.Close(); err != nil {
		return fmt.Errorf("failed closing persistent directory snapshot: %w", err)
	}
	closeTmp = false

	if err = s.swapDatabase(tmpPath, expectedMutation); err != nil {
		return err
	}
	s.writes.Add(uint64(len(paths)))
	return nil
}

func (s *Store) replaceSubtree(ctx context.Context, root string, tree dirtree.DirTree, refreshedAt time.Time, expectedMutation uint64) error {
	paths := sortedTreePaths(tree)
	encoded := make([]encodedDirectory, 0, len(paths))
	for _, dir := range paths {
		data, err := s.encodeDirectory(ctx, dir, tree[dir], refreshedAt)
		if err != nil {
			return err
		}
		encoded = append(encoded, encodedDirectory{path: dir, data: data})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return errStoreClosed
	}
	if s.mutations.Load() != expectedMutation {
		return errChangedDuringTreeRefresh
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketDirs)
		if err := deleteSubtree(bucket, root); err != nil {
			return err
		}
		for _, item := range encoded {
			if err := bucket.Put(directoryKey(item.path), item.data); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketMeta).Put(keySnapshotTime, []byte(strconv.FormatInt(refreshedAt.UnixNano(), 10)))
	})
	if err != nil {
		s.errors.Add(1)
		return fmt.Errorf("failed replacing persistent directory subtree %q: %w", root, err)
	}
	s.writes.Add(uint64(len(encoded)))
	s.mutations.Add(1)
	return nil
}

func (s *Store) swapDatabase(tmpPath string, expectedMutation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	if s.mutations.Load() != expectedMutation {
		return errChangedDuringTreeRefresh
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("failed closing old persistent VFS directory cache: %w", err)
		}
		s.db = nil
	}

	backupPath := s.path + ".old"
	_ = os.Remove(backupPath)
	oldExists := false
	if _, err := os.Stat(s.path); err == nil {
		if err = os.Rename(s.path, backupPath); err != nil {
			_ = s.reopenAfterSwapFailure()
			return fmt.Errorf("failed backing up old persistent VFS directory cache: %w", err)
		}
		oldExists = true
	} else if !os.IsNotExist(err) {
		_ = s.reopenAfterSwapFailure()
		return fmt.Errorf("failed stating old persistent VFS directory cache: %w", err)
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		if oldExists {
			_ = os.Rename(backupPath, s.path)
		}
		_ = s.reopenAfterSwapFailure()
		return fmt.Errorf("failed installing persistent VFS directory cache snapshot: %w", err)
	}

	newDB, err := openDatabase(s.path)
	if err == nil {
		err = initializeDatabase(newDB, s.identity)
	}
	if err != nil {
		if newDB != nil {
			_ = newDB.Close()
		}
		_ = os.Remove(s.path)
		if oldExists {
			_ = os.Rename(backupPath, s.path)
		}
		_ = s.reopenAfterSwapFailure()
		return fmt.Errorf("failed reopening new persistent VFS directory cache: %w", err)
	}
	s.db = newDB
	if oldExists {
		_ = os.Remove(backupPath)
	}
	s.mutations.Add(1)
	return nil
}

func (s *Store) reopenAfterSwapFailure() error {
	if s.closed {
		return errStoreClosed
	}
	db, err := openDatabase(s.path)
	if err != nil {
		return err
	}
	if err = initializeDatabase(db, s.identity); err != nil {
		_ = db.Close()
		return err
	}
	s.db = db
	return nil
}

// InvalidateDirectory removes one directory record from disk.
func (s *Store) InvalidateDirectory(dir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		if s.closed {
			return errStoreClosed
		}
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDirs).Delete(directoryKey(dir))
	})
	if err != nil {
		s.errors.Add(1)
		return fmt.Errorf("failed invalidating persistent directory %q: %w", dir, err)
	}
	s.mutations.Add(1)
	return nil
}

// InvalidateSubtree removes dir and every child directory record from disk.
func (s *Store) InvalidateSubtree(dir string) error {
	dir = cleanRemotePath(dir)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		if s.closed {
			return errStoreClosed
		}
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketDirs)
		if dir == "" {
			if err := tx.DeleteBucket(bucketDirs); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
				return err
			}
			if _, err := tx.CreateBucket(bucketDirs); err != nil {
				return err
			}
			return tx.Bucket(bucketMeta).Delete(keySnapshotTime)
		}
		return deleteSubtree(bucket, dir)
	})
	if err != nil {
		s.errors.Add(1)
		return fmt.Errorf("failed invalidating persistent directory subtree %q: %w", dir, err)
	}
	s.mutations.Add(1)
	return nil
}

// Purge removes every persistent directory record while keeping the store open.
func (s *Store) Purge() error {
	return s.InvalidateSubtree("")
}

func deleteSubtree(bucket *bolt.Bucket, root string) error {
	root = cleanRemotePath(root)
	if root == "" {
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	}

	rootKey := directoryKey(root)
	if err := bucket.Delete(rootKey); err != nil {
		return err
	}

	childPrefix := make([]byte, len(root)+2)
	childPrefix[0] = 1
	copy(childPrefix[1:], root)
	childPrefix[len(childPrefix)-1] = '/'

	// Seek directly to the child prefix. A sibling such as "root-name" sorts
	// between the exact "root" key and "root/child" and must not make us stop
	// before reaching the descendants.
	cursor := bucket.Cursor()
	for key, _ := cursor.Seek(childPrefix); key != nil && bytes.HasPrefix(key, childPrefix); key, _ = cursor.Next() {
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// deleteDescendants deletes cached directories below root while retaining the
// record for root itself.
func deleteDescendants(bucket *bolt.Bucket, root string) error {
	root = cleanRemotePath(root)
	var prefix []byte
	if root == "" {
		prefix = []byte{1}
	} else {
		prefix = make([]byte, len(root)+2)
		prefix[0] = 1
		copy(prefix[1:], root)
		prefix[len(prefix)-1] = '/'
	}

	cursor := bucket.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// deleteReplacedChildSubtrees removes descendants whose direct child
// directory disappeared or was replaced by a directory with another backend
// ID.
func deleteReplacedChildSubtrees(bucket *bolt.Bucket, oldRecord, newRecord directoryRecord) (changed bool, err error) {
	newDirectories := make(map[string]entryRecord)
	for _, entry := range newRecord.Entries {
		if entry.Kind == entryDir {
			newDirectories[cleanRemotePath(entry.Remote)] = entry
		}
	}

	for _, oldEntry := range oldRecord.Entries {
		if oldEntry.Kind != entryDir {
			continue
		}
		child := cleanRemotePath(oldEntry.Remote)
		if !isDirectChild(oldRecord.Path, child) {
			return false, fmt.Errorf("persistent directory %q contains non-child directory entry %q", oldRecord.Path, child)
		}
		newEntry, found := newDirectories[child]
		if found && sameDirectoryIdentity(oldEntry, newEntry) {
			continue
		}
		if err := deleteSubtree(bucket, child); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

func sameDirectoryIdentity(oldEntry, newEntry entryRecord) bool {
	if oldEntry.ID == "" && newEntry.ID == "" {
		return true
	}
	return oldEntry.ID == newEntry.ID
}

func isDirectChild(parent, child string) bool {
	parent = cleanRemotePath(parent)
	child = cleanRemotePath(child)
	if child == "" || child == parent {
		return false
	}
	return cleanRemotePath(path.Dir(child)) == parent
}

func sortedTreePaths(tree dirtree.DirTree) []string {
	paths := make([]string, 0, len(tree))
	for dir := range tree {
		paths = append(paths, cleanRemotePath(dir))
	}
	sort.Strings(paths)
	return paths
}

func (s *Store) encodeDirectory(ctx context.Context, dir string, entries fs.DirEntries, refreshedAt time.Time) ([]byte, error) {
	record := directoryRecord{
		Schema:              recordSchema,
		Path:                cleanRemotePath(dir),
		RefreshedAtUnixNano: refreshedAt.UnixNano(),
		Entries:             make([]entryRecord, 0, len(entries)),
	}
	for _, entry := range entries {
		item := entryRecord{
			Remote: entry.Remote(),
			Size:   entry.Size(),
		}
		modTime := entry.ModTime(ctx)
		item.ModTimeUnixNano, item.ModTimeValid = encodeTime(modTime)
		switch typed := entry.(type) {
		case fs.Object:
			item.Kind = entryObject
			item.Storable = typed.Storable()
			if ider, ok := typed.(fs.IDer); ok {
				item.ID = ider.ID()
			}
		case fs.Directory:
			item.Kind = entryDir
			item.Items = typed.Items()
			item.ID = typed.ID()
		default:
			return nil, fmt.Errorf("can't persist unsupported directory entry type %T", entry)
		}
		if parentIDer, ok := entry.(fs.ParentIDer); ok {
			item.ParentID = parentIDer.ParentID()
		}
		backendData, err := s.codec.EncodePersistentDirEntry(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("backend failed to encode %q for persistent directory cache: %w", entry.Remote(), err)
		}
		if len(backendData) == 0 {
			return nil, fmt.Errorf("backend returned no persistent directory cache data for %q", entry.Remote())
		}
		item.BackendData = backendData
		record.Entries = append(record.Entries, item)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(record); err != nil {
		return nil, fmt.Errorf("failed encoding persistent directory %q: %w", dir, err)
	}
	return buf.Bytes(), nil
}

func decodeDirectoryRecord(data []byte) (directoryRecord, error) {
	var record directoryRecord
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&record); err != nil {
		return record, err
	}
	if record.Schema != recordSchema {
		return record, fmt.Errorf("unsupported directory record schema %d", record.Schema)
	}
	return record, nil
}

func (s *Store) restoreEntries(ctx context.Context, records []entryRecord) (fs.DirEntries, error) {
	entries := make(fs.DirEntries, 0, len(records))
	for _, record := range records {
		isDir := record.Kind == entryDir
		if record.Kind != entryObject && record.Kind != entryDir {
			return nil, fmt.Errorf("unknown persistent entry kind %d for %q", record.Kind, record.Remote)
		}
		if len(record.BackendData) == 0 {
			return nil, fmt.Errorf("persistent entry %q has no backend data", record.Remote)
		}
		entry, err := s.codec.DecodePersistentDirEntry(ctx, record.Remote, isDir, record.BackendData)
		if err != nil {
			return nil, fmt.Errorf("backend failed to decode %q from persistent directory cache: %w", record.Remote, err)
		}
		if entry == nil {
			return nil, fmt.Errorf("backend returned nil while decoding %q from persistent directory cache", record.Remote)
		}
		if isDir {
			if _, ok := entry.(fs.Directory); !ok {
				return nil, fmt.Errorf("backend decoded directory %q as %T", record.Remote, entry)
			}
		} else if _, ok := entry.(fs.Object); !ok {
			return nil, fmt.Errorf("backend decoded object %q as %T", record.Remote, entry)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func encodeTime(t time.Time) (unixNano int64, valid bool) {
	if t.IsZero() {
		return 0, false
	}
	return t.UnixNano(), true
}

func directoryKey(dir string) []byte {
	dir = cleanRemotePath(dir)
	if dir == "" {
		return []byte{0}
	}
	key := make([]byte, len(dir)+1)
	key[0] = 1
	copy(key[1:], dir)
	return key
}

// Stats returns persistent cache information for vfs/stats.
func (s *Store) Stats() rc.Params {
	out := rc.Params{
		"path":      s.path,
		"root":      s.root,
		"identity":  s.identity,
		"hits":      s.hits.Load(),
		"misses":    s.misses.Load(),
		"expired":   s.expired.Load(),
		"errors":    s.errors.Load(),
		"writes":    s.writes.Load(),
		"mutations": s.mutations.Load(),
	}
	if info, err := os.Stat(s.path); err == nil {
		out["bytesUsed"] = info.Size()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		out["open"] = false
		return out
	}
	out["open"] = true
	_ = s.db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket(bucketDirs); bucket != nil {
			out["directories"] = bucket.Stats().KeyN
		}
		if meta := tx.Bucket(bucketMeta); meta != nil {
			if value := meta.Get(keySnapshotTime); value != nil {
				if unixNano, err := strconv.ParseInt(string(value), 10, 64); err == nil {
					out["snapshotTime"] = time.Unix(0, unixNano)
				}
			}
		}
		return nil
	})
	return out
}
