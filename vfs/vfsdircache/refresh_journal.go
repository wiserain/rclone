//go:build !plan9 && !js

package vfsdircache

import (
	"errors"
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
)

const (
	maxTreeRefreshMutations    = 100000
	maxTreeRefreshMutationData = 64 * 1024 * 1024
)

var (
	errTreeRefreshAlreadyRunning  = errors.New("persistent VFS directory cache tree refresh is already running")
	errTreeRefreshJournalOverflow = errors.New("persistent VFS directory cache tree refresh journal exceeded its limit")
	errTreeRefreshTokenInvalid    = errors.New("invalid persistent VFS directory cache tree refresh token")
)

type refreshMutationKind byte

const (
	refreshMutationSaveDirectory refreshMutationKind = iota
	refreshMutationInvalidateDirectory
	refreshMutationInvalidateSubtree
)

type refreshMutation struct {
	kind refreshMutationKind
	path string
	data []byte
}

type treeRefreshSession struct {
	id        uint64
	root      string
	mutations []refreshMutation
	dataBytes int
	overflow  bool
}

// BeginTreeRefresh starts journaling Store changes made during a remote walk.
func (s *Store) BeginTreeRefresh(root string) (TreeRefreshToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil {
		return TreeRefreshToken{}, errStoreClosed
	}
	if s.treeRefresh != nil {
		return TreeRefreshToken{}, errTreeRefreshAlreadyRunning
	}
	s.nextTreeRefreshID++
	s.treeRefresh = &treeRefreshSession{
		id:   s.nextTreeRefreshID,
		root: cleanRemotePath(root),
	}
	return TreeRefreshToken{id: s.treeRefresh.id}, nil
}

// AbortTreeRefresh stops journaling for token without changing the Store.
func (s *Store) AbortTreeRefresh(token TreeRefreshToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.treeRefresh != nil && s.treeRefresh.id == token.id {
		s.treeRefresh = nil
	}
}

func (s *Store) treeRefreshLocked(token TreeRefreshToken, root string) (*treeRefreshSession, error) {
	if s.closed || s.db == nil {
		return nil, errStoreClosed
	}
	if s.treeRefresh == nil || token.id == 0 || s.treeRefresh.id != token.id {
		return nil, errTreeRefreshTokenInvalid
	}
	if s.treeRefresh.root != cleanRemotePath(root) {
		return nil, fmt.Errorf("persistent tree refresh root mismatch: started %q, completed %q", s.treeRefresh.root, root)
	}
	if s.treeRefresh.overflow {
		return nil, errTreeRefreshJournalOverflow
	}
	return s.treeRefresh, nil
}

func (s *Store) appendRefreshMutationLocked(kind refreshMutationKind, path string, data []byte) {
	session := s.treeRefresh
	if session == nil || session.overflow {
		return
	}
	if len(session.mutations) >= maxTreeRefreshMutations || session.dataBytes+len(data) > maxTreeRefreshMutationData {
		session.overflow = true
		return
	}
	mutation := refreshMutation{
		kind: kind,
		path: cleanRemotePath(path),
	}
	if len(data) != 0 {
		mutation.data = append([]byte(nil), data...)
		session.dataBytes += len(mutation.data)
	}
	session.mutations = append(session.mutations, mutation)
}

func replayRefreshMutations(tx *bolt.Tx, mutations []refreshMutation) error {
	bucket := tx.Bucket(bucketDirs)
	for _, mutation := range mutations {
		var err error
		switch mutation.kind {
		case refreshMutationSaveDirectory:
			err = putDirectoryRecord(bucket, mutation.path, mutation.data)
		case refreshMutationInvalidateDirectory:
			err = bucket.Delete(directoryKey(mutation.path))
		case refreshMutationInvalidateSubtree:
			err = deleteSubtree(bucket, mutation.path)
			if err == nil && mutation.path == "" {
				err = tx.Bucket(bucketMeta).Delete(keySnapshotTime)
			}
		default:
			err = fmt.Errorf("unknown persistent tree refresh mutation %d", mutation.kind)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func treeRefreshResult(mutations []refreshMutation) TreeRefreshResult {
	directories := make(map[string]struct{})
	subtrees := make(map[string]struct{})
	for _, mutation := range mutations {
		switch mutation.kind {
		case refreshMutationSaveDirectory, refreshMutationInvalidateDirectory:
			directories[mutation.path] = struct{}{}
		case refreshMutationInvalidateSubtree:
			subtrees[mutation.path] = struct{}{}
		}
	}
	result := TreeRefreshResult{
		StaleDirectories: make([]string, 0, len(directories)),
		StaleSubtrees:    make([]string, 0, len(subtrees)),
	}
	for path := range directories {
		result.StaleDirectories = append(result.StaleDirectories, path)
	}
	for path := range subtrees {
		result.StaleSubtrees = append(result.StaleSubtrees, path)
	}
	sort.Strings(result.StaleDirectories)
	sort.Strings(result.StaleSubtrees)
	return result
}
