package _115 // nolint:revive

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolClientCallBASERotatesClientForRetries(t *testing.T) {
	var firstCalls atomic.Int32
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		firstCalls.Add(1)
		_, _ = w.Write([]byte(`{"state":false,"errno":40110000}`))
	}))
	defer firstServer.Close()

	var secondCalls atomic.Int32
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true}`))
	}))
	defer secondServer.Close()

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 2
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
		"UID=1_B; CID=B; SEID=B;",
	})
	require.NoError(t, err)
	pool.clients[0].SetRoot(firstServer.URL)
	pool.clients[1].SetRoot(secondServer.URL)

	require.NoError(t, pool.CallBASE(ctx, &rest.Opts{Method: http.MethodGet}))
	assert.Equal(t, int32(1), firstCalls.Load())
	assert.Equal(t, int32(1), secondCalls.Load())
}

func TestPoolClientCallBASECoolDownsHTTP405(t *testing.T) {
	var firstCalls atomic.Int32
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"status":405,"message":"Method Not Allowed"}`))
	}))
	defer firstServer.Close()

	var secondCalls atomic.Int32
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true,"errno":0}`))
	}))
	defer secondServer.Close()

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 2
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
		"UID=1_B; CID=B; SEID=B;",
	})
	require.NoError(t, err)
	pool.clients[0].SetRoot(firstServer.URL)
	pool.clients[1].SetRoot(secondServer.URL)

	require.NoError(t, pool.CallBASE(ctx, &rest.Opts{Method: http.MethodGet}))
	assert.Equal(t, int32(1), firstCalls.Load())
	assert.Equal(t, int32(1), secondCalls.Load())

	require.NoError(t, pool.CallBASE(ctx, &rest.Opts{Method: http.MethodGet}))
	assert.Equal(t, int32(1), firstCalls.Load())
	assert.Equal(t, int32(2), secondCalls.Load())

	pool.mu.Lock()
	pool.nextAvailable[0] = time.Now().Add(-time.Second)
	pool.mu.Unlock()
	require.NoError(t, pool.CallBASE(ctx, &rest.Opts{Method: http.MethodGet}))
	assert.Equal(t, int32(2), firstCalls.Load())
	assert.Equal(t, int32(3), secondCalls.Load())
}

func TestPoolClientCallBASEReportsAuthenticationFailureUID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":false,"errno":40101032,"msg":"Please log in again"}`))
	}))
	defer server.Close()

	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 2
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
		"UID=1_B; CID=B; SEID=B;",
	})
	require.NoError(t, err)
	pool.clients[0].SetRoot(server.URL)

	err = pool.CallBASE(ctx, &rest.Opts{Method: http.MethodGet})
	require.Error(t, err)
	assert.True(t, fserrors.IsFatalError(err))
	assert.Contains(t, err.Error(), `cookie UID "1_A"`)
	assert.Contains(t, err.Error(), "API Error(40101032)")
}

func TestPoolClientAllCookiesCoolingDownHonorsContext(t *testing.T) {
	ctx := context.Background()
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2, WaitForCooldown: true}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
		"UID=1_B; CID=B; SEID=B;",
	})
	require.NoError(t, err)
	pool.nextAvailable[0] = time.Now().Add(time.Second)
	pool.nextAvailable[1] = time.Now().Add(time.Second)
	pool.cooldownUntil[0] = pool.nextAvailable[0]
	pool.cooldownUntil[1] = pool.nextAvailable[1]

	callCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = pool.CallJSON(callCtx, &rest.Opts{Method: http.MethodGet}, nil, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestPoolClientAllCookiesCoolingDownReturnsImmediately(t *testing.T) {
	ctx := context.Background()
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
		"UID=1_B; CID=B; SEID=B;",
	})
	require.NoError(t, err)
	pool.nextAvailable[0] = time.Now().Add(time.Second)
	pool.nextAvailable[1] = time.Now().Add(time.Second)
	pool.cooldownUntil[0] = pool.nextAvailable[0]
	pool.cooldownUntil[1] = pool.nextAvailable[1]

	_, err = pool.CallJSON(ctx, &rest.Opts{Method: http.MethodGet}, nil, nil)
	require.Error(t, err)
	assert.EqualError(t, err, "all API cookies are in cooldown")
	assert.True(t, fserrors.IsNoRetryError(err))
}

func TestPoolClientWaitsForMinSleepWhenCooldownWaitIsDisabled(t *testing.T) {
	ctx := context.Background()
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
	})
	require.NoError(t, err)
	pool.nextAvailable[0] = time.Now().Add(20 * time.Millisecond)

	start := time.Now()
	_, _, err = pool.client(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 15*time.Millisecond)
}

func TestPoolClientMaintainsMinSleepDuringCooldown(t *testing.T) {
	ctx := context.Background()
	pool, err := newPoolClient(ctx, &Options{PacerMinSleep: 2, WaitForCooldown: true}, fs.CommaSepList{
		"UID=1_A; CID=A; SEID=A;",
		"UID=1_B; CID=B; SEID=B;",
	})
	require.NoError(t, err)
	pool.nextAvailable[0] = time.Now().Add(time.Second)
	pool.cooldownUntil[0] = pool.nextAvailable[0]
	pool.clientMinSleep = 20 * time.Millisecond

	_, firstIndex, err := pool.client(ctx)
	require.NoError(t, err)
	start := time.Now()
	_, secondIndex, err := pool.client(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, firstIndex)
	assert.Equal(t, 1, secondIndex)
	assert.GreaterOrEqual(t, time.Since(start), 15*time.Millisecond)
}
