package _115 // nolint:revive

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyAPIError(t *testing.T) {
	tests := []struct {
		name   string
		code   api.Int
		action apiErrorAction
		delay  time.Duration
	}{
		{name: "too frequent", code: 590075, action: apiErrorRetry},
		{name: "similar operation pending", code: 990005, action: apiErrorRetry},
		{name: "delete copy or restore pending", code: 990009, action: apiErrorRetry, delay: time.Second},
		{name: "move pending", code: 990019, action: apiErrorRetry, delay: time.Second},
		{name: "request needs retry", code: 40110000, action: apiErrorRetry},
		{name: "login required", code: 99, action: apiErrorFatal},
		{name: "login expired", code: 990001, action: apiErrorFatal},
		{name: "credential invalid", code: 40101032, action: apiErrorFatal},
		{name: "logged out by device management", code: 40101035, action: apiErrorFatal},
		{name: "session exited", code: 40101037, action: apiErrorFatal},
		{name: "empty pickcode", code: 50001, action: apiErrorObjectNotFound},
		{name: "missing pickcode", code: 50003, action: apiErrorObjectNotFound},
		{name: "pickcode deleted", code: 50015, action: apiErrorObjectNotFound},
		{name: "file deleted", code: 70005, action: apiErrorObjectNotFound},
		{name: "duplicate delete", code: 231011, action: apiErrorObjectNotFound},
		{name: "download too large", code: 50028, action: apiErrorNoRetry},
		{name: "cyclic copy", code: 91002, action: apiErrorNoRetry},
		{name: "cyclic move", code: 800006, action: apiErrorNoRetry},
		{name: "invalid share", code: 4100009, action: apiErrorNoRetry},
		{name: "missing share", code: 4100026, action: apiErrorNoRetry},
		{name: "insufficient space", code: 91005, action: apiErrorNoRetry},
		{name: "unhandled error", code: 12345},
		{name: "success", code: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classification := classifyAPIError(test.code)
			assert.Equal(t, test.action, classification.action)
			assert.Equal(t, test.delay, classification.delay)
		})
	}
}

func TestShouldRetryAPIErrorResponseTypes(t *testing.T) {
	tests := []struct {
		name string
		info any
	}{
		{name: "base", info: &api.Base{Errno: 590075}},
		{name: "string info", info: &api.StringInfo{Base: api.Base{Errno: 40110000}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry, err := shouldRetry(context.Background(), nil, test.info, nil)
			assert.True(t, retry)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "API Error")
		})
	}
}

func TestShouldRetryFatalAPIError(t *testing.T) {
	tests := []struct {
		name string
		info any
	}{
		{name: "base", info: &api.Base{Errno: 40101032}},
		{name: "string info", info: &api.StringInfo{Base: api.Base{Errno: 40101032}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry, err := shouldRetry(context.Background(), nil, test.info, nil)
			assert.False(t, retry)
			require.Error(t, err)
			assert.True(t, fserrors.IsFatalError(err))
			assert.Contains(t, err.Error(), "API Error(40101032)")
		})
	}
}

func TestShouldRetryObjectNotFoundAPIError(t *testing.T) {
	tests := []struct {
		name string
		info any
	}{
		{name: "base", info: &api.Base{Errno: 50015}},
		{name: "string info", info: &api.StringInfo{Base: api.Base{Errno: 50015}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry, err := shouldRetry(context.Background(), nil, test.info, nil)
			assert.False(t, retry)
			assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
			assert.Equal(t, fs.ErrorObjectNotFound, err)
		})
	}
}

func TestShouldRetryNoRetryAPIError(t *testing.T) {
	tests := []struct {
		name string
		info any
	}{
		{name: "base", info: &api.Base{Errno: 50028}},
		{name: "string info", info: &api.StringInfo{Base: api.Base{Errno: 50028}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry, err := shouldRetry(context.Background(), nil, test.info, nil)
			assert.False(t, retry)
			require.Error(t, err)
			assert.True(t, fserrors.IsNoRetryError(err))
			assert.Contains(t, err.Error(), "API Error(50028)")
		})
	}
}
