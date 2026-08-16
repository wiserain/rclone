//go:build !plan9 && !js

package vfsdircache

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/dirtree"
	"github.com/stretchr/testify/require"
)

func newRefreshJournalTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), databaseName)
	db, err := openDatabase(path)
	require.NoError(t, err)
	require.NoError(t, initializeDatabase(db, "refresh-journal-test"))
	store := &Store{
		path:     path,
		identity: "refresh-journal-test",
		db:       db,
	}
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func requireDirectoryState(t *testing.T, store *Store, path string, wantFound bool, wantTime time.Time) {
	t.Helper()
	_, gotTime, found, err := store.LoadDirectory(context.Background(), path, 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, wantFound, found)
	if wantFound {
		require.Equal(t, wantTime, gotTime)
	}
}

func TestTreeRefreshReplaysConcurrentMutations(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	ctx := context.Background()
	oldTime := time.Now().Add(-time.Hour).Truncate(time.Nanosecond)
	refreshTime := oldTime.Add(30 * time.Minute)
	concurrentTime := refreshTime.Add(time.Minute)

	require.NoError(t, store.SaveDirectory(ctx, "updated", nil, oldTime))
	require.NoError(t, store.SaveDirectory(ctx, "removed", nil, oldTime))
	require.NoError(t, store.SaveDirectory(ctx, "gone", nil, oldTime))
	require.NoError(t, store.SaveDirectory(ctx, "gone/child", nil, oldTime))

	token, err := store.BeginTreeRefresh("")
	require.NoError(t, err)
	t.Cleanup(func() { store.AbortTreeRefresh(token) })

	require.NoError(t, store.SaveDirectory(ctx, "updated", nil, concurrentTime))
	require.NoError(t, store.InvalidateDirectory("removed"))
	require.NoError(t, store.InvalidateSubtree("gone"))

	tree := dirtree.DirTree{
		"":           nil,
		"updated":    nil,
		"removed":    nil,
		"gone":       nil,
		"gone/child": nil,
		"fresh":      nil,
	}
	result, err := store.ReplaceTree(ctx, "", tree, refreshTime, token)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"updated", "removed"}, result.StaleDirectories)
	require.Equal(t, []string{"gone"}, result.StaleSubtrees)

	requireDirectoryState(t, store, "updated", true, concurrentTime)
	requireDirectoryState(t, store, "removed", false, time.Time{})
	requireDirectoryState(t, store, "gone", false, time.Time{})
	requireDirectoryState(t, store, "gone/child", false, time.Time{})
	requireDirectoryState(t, store, "fresh", true, refreshTime)
}

func TestTreeRefreshRejectsConcurrentRefresh(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	token, err := store.BeginTreeRefresh("one")
	require.NoError(t, err)
	defer store.AbortTreeRefresh(token)

	_, err = store.BeginTreeRefresh("two")
	require.ErrorIs(t, err, errTreeRefreshAlreadyRunning)
}

func TestTreeRefreshStats(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	token, err := store.BeginTreeRefresh("subtree")
	require.NoError(t, err)
	defer store.AbortTreeRefresh(token)
	require.NoError(t, store.InvalidateDirectory("subtree/dir"))

	stats := store.Stats()
	require.Equal(t, true, stats["refreshActive"])
	require.Equal(t, "subtree", stats["refreshRoot"])
	require.Equal(t, 1, stats["refreshMutations"])
	require.Equal(t, 0, stats["refreshJournalBytes"])
	require.Equal(t, false, stats["refreshOverflow"])
}

func TestTreeRefreshAbortAllowsAnotherRefresh(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	token, err := store.BeginTreeRefresh("one")
	require.NoError(t, err)
	store.AbortTreeRefresh(token)

	next, err := store.BeginTreeRefresh("two")
	require.NoError(t, err)
	store.AbortTreeRefresh(next)
}

func TestTreeRefreshDoesNotReplayAutomaticExpiry(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	ctx := context.Background()
	refreshTime := time.Now().Truncate(time.Nanosecond)
	require.NoError(t, store.SaveDirectory(ctx, "dir", nil, refreshTime.Add(-time.Hour)))

	token, err := store.BeginTreeRefresh("")
	require.NoError(t, err)
	t.Cleanup(func() { store.AbortTreeRefresh(token) })
	require.NoError(t, store.ExpireSubtree("dir"))

	_, err = store.ReplaceTree(ctx, "", dirtree.DirTree{"": nil, "dir": nil}, refreshTime, token)
	require.NoError(t, err)
	requireDirectoryState(t, store, "dir", true, refreshTime)
}

func TestTreeRefreshReplaysRootInvalidation(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	ctx := context.Background()
	token, err := store.BeginTreeRefresh("")
	require.NoError(t, err)
	t.Cleanup(func() { store.AbortTreeRefresh(token) })
	require.NoError(t, store.InvalidateSubtree(""))

	result, err := store.ReplaceTree(ctx, "", dirtree.DirTree{"": nil, "dir": nil}, time.Now(), token)
	require.NoError(t, err)
	require.Equal(t, []string{""}, result.StaleSubtrees)
	require.Equal(t, 0, store.Stats()["directories"])
	require.NotContains(t, store.Stats(), "snapshotTime")
}

func TestTreeRefreshReplaysSubtreeMutations(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	ctx := context.Background()
	oldTime := time.Now().Add(-time.Hour).Truncate(time.Nanosecond)
	refreshTime := oldTime.Add(30 * time.Minute)
	concurrentTime := refreshTime.Add(time.Minute)
	require.NoError(t, store.SaveDirectory(ctx, "tree/updated", nil, oldTime))
	require.NoError(t, store.SaveDirectory(ctx, "tree/removed", nil, oldTime))
	require.NoError(t, store.SaveDirectory(ctx, "outside", nil, oldTime))

	token, err := store.BeginTreeRefresh("tree")
	require.NoError(t, err)
	t.Cleanup(func() { store.AbortTreeRefresh(token) })
	require.NoError(t, store.SaveDirectory(ctx, "tree/updated", nil, concurrentTime))
	require.NoError(t, store.InvalidateDirectory("tree/removed"))
	require.NoError(t, store.SaveDirectory(ctx, "outside", nil, concurrentTime))

	tree := dirtree.DirTree{
		"tree":         nil,
		"tree/updated": nil,
		"tree/removed": nil,
		"tree/fresh":   nil,
	}
	_, err = store.ReplaceTree(ctx, "tree", tree, refreshTime, token)
	require.NoError(t, err)
	requireDirectoryState(t, store, "tree/updated", true, concurrentTime)
	requireDirectoryState(t, store, "tree/removed", false, time.Time{})
	requireDirectoryState(t, store, "tree/fresh", true, refreshTime)
	requireDirectoryState(t, store, "outside", true, concurrentTime)
}

func TestTreeRefreshJournalOverflowPreservesCurrentDatabase(t *testing.T) {
	store := newRefreshJournalTestStore(t)
	ctx := context.Background()
	oldTime := time.Now().Add(-time.Hour).Truncate(time.Nanosecond)
	refreshTime := oldTime.Add(30 * time.Minute)
	require.NoError(t, store.SaveDirectory(ctx, "current", nil, oldTime))

	token, err := store.BeginTreeRefresh("")
	require.NoError(t, err)
	t.Cleanup(func() { store.AbortTreeRefresh(token) })
	store.mu.Lock()
	store.treeRefresh.overflow = true
	store.mu.Unlock()

	_, err = store.ReplaceTree(ctx, "", dirtree.DirTree{"": nil, "replacement": nil}, refreshTime, token)
	require.ErrorIs(t, err, errTreeRefreshJournalOverflow)
	requireDirectoryState(t, store, "current", true, oldTime)
	requireDirectoryState(t, store, "replacement", false, time.Time{})
}
