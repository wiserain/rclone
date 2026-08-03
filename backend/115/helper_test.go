package _115 // nolint:revive

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckDownloadURL(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		durl, err := checkDownloadURL(nil, &http.Response{})
		assert.Nil(t, durl)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	})

	t.Run("empty", func(t *testing.T) {
		durl, err := checkDownloadURL(&api.DownloadURL{}, &http.Response{})
		assert.Nil(t, durl)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	})

	t.Run("valid", func(t *testing.T) {
		want := &api.DownloadURL{URL: "https://example.com/file"}
		resp := &http.Response{Header: http.Header{
			"Set-Cookie": {"download_token=token"},
		}}

		got, err := checkDownloadURL(want, resp)
		require.NoError(t, err)
		assert.Same(t, want, got)
		require.Len(t, got.Cookies, 1)
		assert.Equal(t, "download_token", got.Cookies[0].Name)
		assert.Equal(t, "token", got.Cookies[0].Value)
	})
}

func TestDownloadURLValidRejectsEmptyURL(t *testing.T) {
	assert.False(t, (*api.DownloadURL)(nil).Valid())
	assert.False(t, (&api.DownloadURL{}).Valid())
	assert.True(t, (&api.DownloadURL{URL: "https://example.com/file"}).Valid())
}

func TestDownloadURLUserAgentMatchesDownload(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
	}{
		{name: "configured", userAgent: defaultUserAgent},
		{name: "empty"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			userAgents := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				userAgents <- r.UserAgent()
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/downurl" {
					_, _ = w.Write([]byte(`{"state":true}`))
				}
			}))
			defer server.Close()

			ctx, ci := fs.AddConfig(context.Background())
			ci.UserAgent = test.userAgent
			client := rest.NewClient(getClient(ctx, &Options{})).SetRoot(server.URL)

			var info api.Base
			_, err := client.CallJSON(ctx, &rest.Opts{
				Method: http.MethodPost,
				Path:   "/downurl",
			}, nil, &info)
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/file", nil)
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			apiUserAgent := <-userAgents
			downloadUserAgent := <-userAgents
			assert.Equal(t, test.userAgent, apiUserAgent)
			assert.Equal(t, apiUserAgent, downloadUserAgent)
		})
	}
}

func TestCheckDownloadAPI(t *testing.T) {
	require.NoError(t, checkDownloadAPI(downloadAPIChrome))
	require.NoError(t, checkDownloadAPI(downloadAPIAndroid))
	require.Error(t, checkDownloadAPI("auto"))
	require.Error(t, checkDownloadAPI(""))
}

func TestDownloadAPIEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		downloadAPI string
		rootURL     string
		pickCodeKey string
	}{
		{
			name:        "chrome",
			downloadAPI: downloadAPIChrome,
			rootURL:     "https://proapi.115.com/app/chrome/downurl",
			pickCodeKey: "pickcode",
		},
		{
			name:        "android",
			downloadAPI: downloadAPIAndroid,
			rootURL:     "https://proapi.115.com/android/2.0/ufile/download",
			pickCodeKey: "pick_code",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rootURL, pickCodeKey := downloadAPIEndpoint(test.downloadAPI)
			assert.Equal(t, test.rootURL, rootURL)
			assert.Equal(t, test.pickCodeKey, pickCodeKey)
		})
	}
}
