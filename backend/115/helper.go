package _115

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*api.File) bool

// listOrder sets order of directory listing
func (f *Fs) listOrder(ctx context.Context, cid, order, asc string) (err error) {
	form := url.Values{}
	form.Set("file_id", cid)
	form.Set("user_order", order)
	form.Set("user_asc", asc)
	form.Set("fc_mix", "0")

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/order",
		MultipartParams: form,
	}
	return f.srv.CallBASE(ctx, &opts)
}

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
func (f *Fs) listAll(ctx context.Context, dirID string, limit int, filesOnly, dirsOnly bool, fn listAllFn) (found bool, err error) {
	if f.isShare {
		return f.listShare(ctx, dirID, limit, fn)
	}
	order := "user_ptime"
	asc := "0"

	// Url Parameters
	params := listParams(dirID, limit)
	params.Set("o", order)
	params.Set("asc", asc)

	offset := 0
	retries := 0 // to prevent infinite loop
OUTER:
	for {
		params.Set("offset", strconv.Itoa(offset))

		info, err := f.getFiles(ctx, params)
		if err != nil {
			return found, fmt.Errorf("couldn't get files: %w", err)
		}
		if info.Count == 0 {
			break
		}
		if filesOnly && info.FileCount == 0 {
			break
		}
		if dirsOnly && info.FolderCount == 0 {
			break
		}
		if order != info.Order || asc != info.IsAsc.String() {
			if retries > 3 {
				return found, fmt.Errorf("max retries exceeded for setting list order")
			}
			if ordErr := f.listOrder(ctx, dirID, order, asc); ordErr != nil {
				return found, fmt.Errorf("failed to set list order: %w", ordErr)
			}
			retries++
			continue // retry with same offset
		}
		for _, item := range info.Files {
			isDir := item.IsDir()
			if filesOnly && isDir {
				continue
			}
			if dirsOnly && !isDir {
				continue
			}
			if !isDir && f.opt.CensoredOnly && item.Censored == 0 {
				continue
			}
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				found = true
				break OUTER
			}
		}
		offset = info.Offset + len(info.Files)
		if offset >= info.Count {
			break
		}
	}
	return
}

// listParams generates a default parameter set for list API
func listParams(dirID string, limit int) url.Values {
	params := url.Values{}
	params.Set("aid", "1")
	params.Set("cid", dirID)
	params.Set("o", "user_ptime") // following options are avaialbe for listing order
	// * file_name
	// * file_size
	// * file_type
	// * user_ptime (create_time) == sorted by tp
	// * user_utime (modify_time) == sorted by te
	// * user_otime (last_opened) == sorted by to
	params.Set("asc", "0")      // ascending order "0" or "1"
	params.Set("show_dir", "1") // this is not for showing dirs_only. It will list all files in dir recursively if "0".
	params.Set("limit", strconv.Itoa(limit))
	params.Set("snap", "0")
	params.Set("record_open_time", "1")
	params.Set("count_folders", "1")
	params.Set("format", "json")
	params.Set("fc_mix", "0")
	params.Set("offset", "0")
	return params
}

// getFiles fetches a single chunk of file lists filtered by the given parameters
func (f *Fs) getFiles(ctx context.Context, params url.Values) (info *api.FileList, err error) {
	opts := rest.Opts{
		Method:     "GET",
		RootURL:    "https://webapi.115.com/files",
		Parameters: params,
	}
	if params.Get("o") == "file_name" {
		params.Set("natsort", "1")
		opts.RootURL = "https://aps.115.com/natsort/files.php"
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	}
	return info, info.Err()
}

// getDirPath returns an absolute path of dirID
func (f *Fs) getDirPath(ctx context.Context, dirID string) (dir string, err error) {
	if dirID == "0" {
		return "", nil
	}
	info, err := f.getFiles(ctx, listParams(dirID, 32))
	if err != nil {
		return "", fmt.Errorf("couldn't get files: %w", err)
	}
	for _, p := range info.Path {
		if p.CID.String() == "0" {
			continue
		}
		dir = path.Join(dir, f.opt.Enc.ToStandardName(p.Name))
	}
	return
}

func (f *Fs) makeDir(ctx context.Context, pid, name string) (info *api.NewDir, err error) {
	form := url.Values{}
	form.Set("pid", pid)
	form.Set("cname", f.opt.Enc.FromStandardName(name))

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/add",
		MultipartParams: form,
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	}
	if err = info.Err(); err != nil {
		if info.ErrCode() == 20004 {
			return nil, fs.ErrorDirExists
		}
		return nil, err
	}
	return
}

func (f *Fs) renameFile(ctx context.Context, fid, newName string) (err error) {
	form := url.Values{}
	form.Set("fid", fid)
	form.Set("file_name", newName)
	form.Set(fmt.Sprintf("files_new_name[%s]", fid), newName)

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/batch_rename",
		MultipartParams: form,
	}
	return f.srv.CallBASE(ctx, &opts)
}

func (f *Fs) deleteFiles(ctx context.Context, fids []string) (err error) {
	form := url.Values{}
	for i, fid := range fids {
		form.Set(fmt.Sprintf("fid[%d]", i), fid)
	}
	// form.Set("pid", pid)
	form.Set("ignore_warn", "1")

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/rb/delete",
		MultipartParams: form,
	}
	return f.srv.CallBASE(ctx, &opts)
}

func (f *Fs) moveFiles(ctx context.Context, fids []string, pid string) (err error) {
	form := url.Values{}
	for i, fid := range fids {
		form.Set(fmt.Sprintf("fid[%d]", i), fid)
	}
	form.Set("pid", pid)
	form.Set("ignore_warn", "1")

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/move",
		MultipartParams: form,
	}
	return f.srv.CallBASE(ctx, &opts)
}

func (f *Fs) copyFiles(ctx context.Context, fids []string, pid string) (err error) {
	form := url.Values{}
	for i, fid := range fids {
		form.Set(fmt.Sprintf("fid[%d]", i), fid)
	}
	form.Set("pid", pid)

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/files/copy",
		MultipartParams: form,
	}
	return f.srv.CallBASE(ctx, &opts)
}

func (f *Fs) indexInfo(ctx context.Context) (data *api.IndexData, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/files/index_info",
	}

	var info *api.IndexInfo
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	}
	if err = info.Err(); err != nil {
		return
	}
	if data = info.Data; data == nil {
		return nil, errors.New("no data")
	}
	return
}

func (f *Fs) _getDownloadURL(ctx context.Context, request interface{}, response interface{}) (resp *http.Response, err error) {
	rootURL := "https://proapi.115.com/app/chrome/downurl"
	if f.isShare {
		rootURL = "https://proapi.115.com/app/share/downurl"
	}
	t := strconv.Itoa(int(time.Now().Unix()))
	opts := rest.Opts{
		Method:     "POST",
		RootURL:    rootURL,
		Parameters: url.Values{"t": {t}},
	}
	return f.dsrv.CallDATA(ctx, &opts, request, response)
}

func (f *Fs) getDownloadURL(ctx context.Context, pickCode string) (durl *api.DownloadURL, err error) {
	req := map[string]string{"pickcode": pickCode}
	downData := api.DownloadData{}
	resp, err := f._getDownloadURL(ctx, req, &downData)
	if err != nil {
		return
	}
	for _, downInfo := range downData {
		durl = &downInfo.URL
		durl.Cookies = resp.Cookies()
		return
	}
	return nil, fs.ErrorObjectNotFound
}

// Looks up a directory ID using its absolute path.
//
// The input directory path should begin with a forward slash.
// The output from API calls will be "0" if the path does not exist or is a file.
func (f *Fs) getDirID(ctx context.Context, dir string) (cid string, err error) {
	dir = strings.TrimPrefix(dir, "/")
	if dir == "" {
		return "0", nil
	}
	params := url.Values{}
	params.Set("path", f.opt.Enc.FromStandardPath("/"+dir))
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/files/getid",
		Parameters: params,
	}

	var info *api.DirID
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	}
	if err = info.Err(); err != nil {
		return
	}
	cid = info.ID.String()
	if cid == "0" && dir != "/" {
		return "", fs.ErrorDirNotFound
	}
	return
}

// getFile gets information of a file or directory by its ID or pickCode
func (f *Fs) getFile(ctx context.Context, fid, pc string) (file *api.File, err error) {
	if fid == "0" {
		return nil, errors.New("can't get information about root directory")
	}
	params := url.Values{}
	if fid != "" {
		params.Set("file_id", fid)
	}
	if pc != "" {
		params.Set("pick_code", pc)
	}
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/files/get_info",
		Parameters: params,
	}

	var info *api.FileInfo
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	}
	if err = info.Err(); err != nil {
		return
	}
	if len(info.Data) > 0 {
		file = info.Data[0]
		file.Name = f.opt.Enc.ToStandardName(file.Name)
		return
	}
	return nil, fmt.Errorf("no data")
}

// getStats gets information of a file or directory by its ID
//
// Note that the process can be quite slow, depending on the number of file objects.
func (f *Fs) getStats(ctx context.Context, cid string) (info *api.FileStats, err error) {
	if cid == "0" {
		return nil, errors.New("can't get information about root directory")
	}
	params := url.Values{}
	params.Set("cid", cid)
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/category/get",
		Parameters: params,
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	}
	info.FileName = f.opt.Enc.ToStandardName(info.FileName)
	for n, parent := range info.Paths {
		info.Paths[n].FileName = f.opt.Enc.ToStandardName(parent.FileName)
	}
	return
}

// ------------------------------------------------------------

// add offline download task for multiple urls
func (f *Fs) addURLs(ctx context.Context, dir string, urls []string) (info *api.NewURL, err error) {
	parentID, _ := f.dirCache.FindDir(ctx, dir, false)
	payload := map[string]string{
		"ac":         "add_task_urls",
		"app_ver":    f.appVer,
		"uid":        f.userID,
		"wp_path_id": parentID,
	}
	for ind, url := range urls {
		payload[fmt.Sprintf("url[%d]", ind)] = url
	}

	opts := rest.Opts{
		Method:     "POST",
		RootURL:    "https://lixian.115.com/lixianssp/",
		Parameters: url.Values{"ac": {"add_task_urls"}},
	}

	_, err = f.srv.CallDATA(ctx, &opts, payload, &info)
	return
}

// ------------------------------------------------------------

// parses arguments for Shared from following URL pattern
//
// https://115.com/s/{shareCode}?password={receiveCode}
func parseShareLink(rawURL string) (shareCode, receiveCode string, err error) {
	if !strings.HasPrefix(rawURL, "http") || !strings.Contains(rawURL, "/s/") {
		return "", "", fmt.Errorf("%q is not a share link", rawURL)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid share link format: %w", err)
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", "", fmt.Errorf("invalid share link format: %w", err)
	}
	return strings.TrimPrefix(u.Path, "/s/"), q.Get("password"), nil
}

// listing filesystem from share link
//
// no need user authorization by cookies
func (f *Fs) listShare(ctx context.Context, dirID string, limit int, fn listAllFn) (found bool, err error) {
	// Url Parameters
	params := url.Values{}
	params.Set("share_code", f.opt.ShareCode)
	params.Set("receive_code", f.opt.ReceiveCode)
	params.Set("cid", dirID)
	params.Set("limit", strconv.Itoa(limit))

	opts := rest.Opts{
		Method:     "GET",
		Path:       "/share/snap",
		Parameters: params,
	}

	offset := 0
OUTER:
	for {
		params.Set("offset", strconv.Itoa(offset))

		var info *api.ShareSnap
		var resp *http.Response
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
			return shouldRetry(ctx, resp, info, err)
		})
		if err != nil {
			return found, fmt.Errorf("couldn't list files: %w", err)
		}
		if err = info.Err(); err != nil {
			return
		}
		if len(info.Data.List) == 0 {
			break
		}
		for _, item := range info.Data.List {
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				found = true
				break OUTER
			}
		}
		offset += f.opt.ListChunk
		if offset >= info.Data.Count {
			break
		}
	}
	return
}

// copyFromShare copies shared object by its shareCode, receiveCode, fid
//
// fid = "0" or "" means root directory containing all files/dirs
func (f *Fs) copyFromShare(ctx context.Context, shareCode, receiveCode, fid, cid string) (err error) {
	form := url.Values{}
	form.Set("share_code", shareCode)     // src
	form.Set("receive_code", receiveCode) // src
	form.Set("file_id", fid)              // src
	form.Set("cid", cid)                  // dst
	form.Set("user_id", f.userID)         // dst

	opts := rest.Opts{
		Method:          "POST",
		Path:            "/share/receive",
		MultipartParams: form,
	}
	return f.srv.CallBASE(ctx, &opts)
}

func (f *Fs) copyFromShareSrc(ctx context.Context, src fs.Object, cid string) (err error) {
	srcObj, _ := src.(*Object) // this is already checked
	return f.copyFromShare(ctx, srcObj.fs.opt.ShareCode, srcObj.fs.opt.ReceiveCode, srcObj.id, cid)
}

func (f *Fs) getDownloadURLFromShare(ctx context.Context, fid string) (durl *api.DownloadURL, err error) {
	req := map[string]string{
		"share_code":   f.opt.ShareCode,
		"receive_code": f.opt.ReceiveCode,
		"file_id":      fid,
	}
	downInfo := api.ShareDownloadInfo{}
	resp, err := f._getDownloadURL(ctx, req, &downInfo)
	if err != nil {
		return
	}
	durl = &downInfo.URL
	durl.Cookies = resp.Cookies()
	return
}
