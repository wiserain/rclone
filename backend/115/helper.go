package _115

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/lib/rest"
)

func (f *Fs) getUploadInfo(ctx context.Context) (info *api.UploadInfo, err error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://proapi.115.com/app/uploadinfo",
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get uploadinfo: %w", err)
	}
	return
}
