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
