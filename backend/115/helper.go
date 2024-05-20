package _115

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

const (
	ListLimit = 100
)

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*api.File) bool

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
func (f *Fs) listAll(ctx context.Context, dirID string, fn listAllFn) (found bool, err error) {
	// Url Parameters
	params := url.Values{}
	params.Set("aid", "1")
	params.Set("cid", dirID)
	params.Set("o", "user_ptime") // order by time
	params.Set("asc", "0")        // ascending order?
	params.Set("show_dir", "1")
	params.Set("limit", strconv.Itoa(ListLimit))
	params.Set("snap", "0")
	params.Set("natsort", "1")
	params.Set("record_open_time", "1")
	params.Set("count_folders", "1")
	params.Set("format", "json")
	params.Set("fc_mix", "0")

	opts := rest.Opts{
		Method:     "GET",
		Path:       "/files",
		Parameters: params,
	}

	offset := 0
OUTER:
	for {
		params.Set("offset", strconv.Itoa(offset))

		var info api.FileList
		var resp *http.Response
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return found, fmt.Errorf("couldn't list files: %w", err)
		}
		if len(info.Files) == 0 {
			break
		}
		for _, item := range info.Files {
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				found = true
				break OUTER
			}
		}
		offset = info.Offset + ListLimit
		if offset >= info.Count {
			break
		}
	}
	return
}

func (f *Fs) makeDir(ctx context.Context, pid, name string) (info *api.NewDir, err error) {
	params := url.Values{}
	params.Set("pid", pid)
	params.Set("cname", f.opt.Enc.FromStandardName(name))

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/add",
		MultipartParams: params,
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return
	}
	if !info.State {
		if info.Errno == "20004" {
			return nil, fs.ErrorDirExists
		}
		return nil, fmt.Errorf("failed to make a new dir: %s (%s)", info.Error, info.Errno)
	}
	return
}

func (f *Fs) renameFile(ctx context.Context, fid, newName string) (info *api.Base, err error) {
	params := url.Values{}
	params.Set("fid", fid)
	params.Set("file_name", newName)
	params.Set(fmt.Sprintf("files_new_name[%s]", fid), newName)

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/batch_rename",
		MultipartParams: params,
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return
	}
	if !info.State {
		return nil, fmt.Errorf("failed to rename: %s (%v)", info.Error, info.Errno)
	}
	return
}

func (f *Fs) deleteFile(ctx context.Context, fid, pid string) (info *api.Base, err error) {
	params := url.Values{}
	params.Set("fid[0]", fid)
	params.Set("pid", pid)
	params.Set("ignore_warn", "1")

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/rb/delete",
		MultipartParams: params,
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return
	}
	if !info.State {
		if errno, ok := info.Errno.(int64); ok && errno == 990009 {
			time.Sleep(time.Second)
		}
		return nil, fmt.Errorf("failed to delete: %s (%v)", info.Error, info.Errno)
	}
	return
}

func (f *Fs) moveFile(ctx context.Context, fid, pid string) (info *api.Base, err error) {
	params := url.Values{}
	params.Set("fid[0]", fid)
	params.Set("pid", pid)

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/move",
		MultipartParams: url.Values{},
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return
	}
	if !info.State {
		return nil, fmt.Errorf("failed to move: %s (%v)", info.Error, info.Errno)
	}
	return
}

func (f *Fs) copyFiles(ctx context.Context, fids []string, pid string) (err error) {
	if len(fids) == 0 {
		return
	}
	params := url.Values{}
	params.Set("pid", pid)
	for i, fid := range fids {
		params.Set(fmt.Sprintf("fid[%d]", i), fid)
	}

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/copy",
		MultipartParams: url.Values{},
	}

	var info *api.Base
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err == nil && !info.State {
		return fmt.Errorf("failed to move: %s (%v)", info.Error, info.Errno)
	}
	return
}

func (f *Fs) indexInfo(ctx context.Context) (info *api.IndexInfo, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/files/index_info",
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	return
}
