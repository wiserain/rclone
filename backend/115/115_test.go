package _115 // nolint:revive

import (
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName:      "Test115:",
		NilObject:       (*Object)(nil),
		SkipInvalidUTF8: true,
		ChunkedUpload: fstests.ChunkedUploadConfig{
			MinChunkSize: minChunkSize,
			MaxChunkSize: maxChunkSize,
		},
	})
}

func TestCheckUploadCutoff(t *testing.T) {
	tests := []struct {
		name    string
		cutoff  fs.SizeSuffix
		wantErr bool
	}{
		{name: "below maximum", cutoff: maxUploadCutoff - 1},
		{name: "maximum", cutoff: maxUploadCutoff},
		{name: "above maximum", cutoff: maxUploadCutoff + 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkUploadCutoff(test.cutoff)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected upload cutoff validation error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected upload cutoff validation error: %v", err)
			}
		})
	}
}

func (f *Fs) SetUploadChunkSize(cs fs.SizeSuffix) (fs.SizeSuffix, error) {
	return f.setUploadChunkSize(cs)
}

func (f *Fs) SetUploadCutoff(cs fs.SizeSuffix) (fs.SizeSuffix, error) {
	return f.setUploadCutoff(cs)
}

var (
	_ fstests.SetUploadChunkSizer = (*Fs)(nil)
	_ fstests.SetUploadCutoffer   = (*Fs)(nil)
)
