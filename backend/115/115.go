package _115

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/backend/115/crypto"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

// Constants
const (
	domain    = "www.115.com"
	userAgent = "Mozilla/5.0 115Browser/23.9.3.2"
	rootURL   = "https://webapi.115.com"

	minSleep      = 150 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "115",
		Description: "115 drive",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "uid",
			Help:      "UID from cookie",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "cid",
			Help:      "CID from cookie",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "seid",
			Help:      "SEID from cookie",
			Required:  true,
			Sensitive: true,
		}, {
			Name: "root_folder_id",
			Help: `ID of the root folder.
Leave blank normally.

Fill in for rclone to use a non root folder as its starting point.
`,
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeLeftSpace |
				encoder.EncodeRightSpace |
				encoder.EncodeBackSlash |
				encoder.EncodeColon |
				encoder.EncodeAsterisk |
				encoder.EncodeQuestion |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLtGt |
				encoder.EncodePipe |
				encoder.EncodePercent |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configguration of this backend
type Options struct {
	UID          string               `config:"uid"`
	CID          string               `config:"cid"`
	SEID         string               `config:"seid"`
	RootFolderID string               `config:"root_folder_id"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote 115 drive
type Fs struct {
	name         string
	root         string
	opt          Options
	features     *fs.Features
	srv          *rest.Client
	dirCache     *dircache.DirCache // Map of directory path to directory id
	pacer        *fs.Pacer
	rootFolderID string
	cache        *cache.Cache
}

// Object describes a 115 object
type Object struct {
	fs          *Fs
	remote      string
	hasMetaData bool
	id          string
	parent      string
	size        int64
	sha1sum     string
	pickCode    string
	mTime       time.Time // modified/updated at
	cTime       time.Time // created at
	aTime       time.Time // last accessed at
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if err == nil {
		return false, nil
	}
	if fserrors.ShouldRetry(err) {
		return true, err
	}
	authRetry := false

	// TODO

	return authRetry || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// ------------------------------------------------------------

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	// TODO
	// // Decode error response
	// errResponse := new(api.Error)
	// err := rest.DecodeJSON(resp, &errResponse)
	// if err != nil {
	// 	fs.Debugf(nil, "Couldn't decode error response: %v", err)
	// }
	// if errResponse.Reason == "" {
	// 	errResponse.Reason = resp.Status
	// }
	// if errResponse.Code == 0 {
	// 	errResponse.Code = resp.StatusCode
	// }
	// return errResponse
	body, err := rest.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("error reading error out of body: %w", err)
	}
	if resp.StatusCode == http.StatusForbidden {
		time.Sleep(time.Second)
	}
	return fmt.Errorf("HTTP error %v (%v) returned body: %q", resp.StatusCode, resp.Status, body)
}

// newClientWithPacer sets a new http/rest client with a pacer to Fs
func (f *Fs) newClientWithPacer(ctx context.Context) (err error) {
	f.srv = rest.NewClient(&http.Client{}).SetRoot(rootURL).SetErrorHandler(errorHandler)
	f.srv.SetHeader("User-Agent", userAgent).SetCookie(&http.Cookie{
		Name:     "UID",
		Value:    f.opt.UID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "CID",
		Value:    f.opt.CID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "SEID",
		Value:    f.opt.SEID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	})
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	f.pacer.SetMaxConnections(2)
	return nil
}

// newFs partially constructs Fs from the path
//
// It constructs a valid Fs but doesn't attempt to figure out whether
// it is a file or a directory.
func newFs(ctx context.Context, name, path string, m configmap.Mapper) (*Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}

	root := strings.Trim(path, "/")

	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		cache: cache.New(time.Minute, time.Minute*2),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true, // can have empty directories
	}).Fill(ctx, f)

	if err := f.newClientWithPacer(ctx); err != nil {
		return nil, err
	}

	return f, nil
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, path string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, path, m)
	if err != nil {
		return nil, err
	}

	// Set the root folder ID
	if f.opt.RootFolderID != "" {
		// use root_folder ID if set
		f.rootFolderID = f.opt.RootFolderID
	} else {
		f.rootFolderID = "0" //根目录 = root directory
	}

	f.dirCache = dircache.New(f.root, f.rootFolderID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(f.root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, f.rootFolderID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("115 %s", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// Hashes returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA1)
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
//
// If remote points to a directory then it should return
// ErrorIsDir if possible without doing any extra work,
// otherwise ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	// Find the leaf in pathID
	found, err = f.listAll(ctx, pathID, func(item *api.File) bool {
		if item.Name == leaf && item.FID == "" {
			pathIDOut = item.CID
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// fs.Debugf(f, "List(%q)\n", dir)
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	var iErr error
	_, err = f.listAll(ctx, dirID, func(item *api.File) bool {
		entry, err := f.itemToDirEntry(ctx, path.Join(dir, item.Name), item)
		if err != nil {
			iErr = err
			return true
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if iErr != nil {
		return nil, iErr
	}
	return entries, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	info, err := f.makeDir(ctx, pathID, leaf)
	if err != nil {
		return
	}
	return info.CID, nil
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() == 0 {
		return nil, fs.ErrorCantUploadEmptyFiles
	}
	// // TODO: enable file upload
	// if true {
	// 	return nil, fs.ErrorNotImplemented
	// }

	o := f.createObject(src.Remote(), src.ModTime(ctx), src.Size())
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Move src to this remote using server-side move operations.
//
// # This is stored with the remote path given
//
// # It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantMove
	// if src.Fs().Name() != f.Name() {
	// 	return nil, fs.ErrorCantMove
	// }

	// srcObj, ok := src.(*Object)
	// if !ok {
	// 	fs.Errorf(f, "can not move, not same remote type")
	// 	return nil, fs.ErrorCantMove
	// }

	// srcParent, srcName := path.Split(f.remotePath(srcObj.remote))
	// dstParent, dstName := path.Split(f.remotePath(remote))
	// if srcParent == dstParent {
	// 	if srcName == dstName {
	// 		return srcObj, nil
	// 	}

	// 	err := f.renameFile(ctx, srcObj.id, dstName)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// } else {
	// 	if srcObj.remote != dstName {
	// 		return nil, fs.ErrorCantMove
	// 	}

	// 	cid, err := f.getDirID(ctx, dstParent)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	err = f.moveFile(ctx, srcObj.id, cid)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// f.flushDir(srcParent)
	// f.flushDir(dstParent)
	// return f.NewObject(ctx, remote)
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	if src.Name() != f.Name() {
		return fs.ErrorCantDirMove
	}

	srcFs := src.(*Fs)
	cid, err := f.getDirID(ctx, srcFs.remotePath(srcRemote))
	if err != nil {
		return err
	}

	srcParent, srcName := path.Split(srcFs.remotePath(srcRemote))
	dstParent, dstName := path.Split(f.remotePath(dstRemote))
	if srcParent == dstParent {
		if srcName == dstName {
			return fs.ErrorDirExists
		}

		err = f.renameFile(ctx, cid, dstName)
		if err != nil {
			return err
		}
	} else {
		pid, err := f.getDirID(ctx, dstParent)
		if errors.Is(err, fs.ErrorDirNotFound) {
			newDir, _ := path.Split(path.Clean(dstRemote))
			err = f.Mkdir(ctx, newDir)
			if err != nil {
				return err
			}
			pid, err = f.getDirID(ctx, dstParent)
		}
		if err != nil {
			return err
		}

		err = f.moveFile(ctx, cid, pid)
		if err != nil {
			return err
		}
		if srcName != dstName {
			err = f.renameFile(ctx, cid, dstName)
			if err != nil {
				return err
			}
		}
	}

	for _, dir := range []string{srcParent, dstParent, srcFs.remotePath(srcRemote), f.remotePath(dstRemote)} {
		f.flushDir(dir)
		srcFs.flushDir(dir)
	}
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return nil
	// files, err := f.readDir(ctx, f.remotePath(dir))
	// if err != nil {
	// 	return err
	// }

	// if len(files) > 0 {
	// 	fs.Logf(f, "rmdir, is not empty, dir: %v", dir)
	// 	return fs.ErrorDirectoryNotEmpty
	// }

	// return f.Purge(ctx, dir)
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	// info, err := f.readMetaDataForPath(ctx, f.remotePath(dir))
	// if err != nil {
	// 	fs.Errorf(f, "purge fail, err: %v, dir: %v", err, f.remotePath(dir))
	// 	if errors.Is(err, fs.ErrorObjectNotFound) {
	// 		return fs.ErrorDirNotFound
	// 	}
	// 	return err
	// }
	// if !info.IsDir() {
	// 	return fs.ErrorIsFile
	// }
	// if info.GetCategoryID() == 0 {
	// 	fs.Logf(f, "is root dir, can not purge")
	// 	return nil
	// }

	// err = f.deleteFile(ctx, info.GetCategoryID(), info.GetParentID())
	// if err != nil {
	// 	return err
	// }

	// parent, _ := path.Split(f.remotePath(dir))
	// f.flushDir(parent)

	return nil
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	info, err := f.indexInfo(ctx)
	if err != nil {
		return nil, err
	}

	usage := &fs.Usage{}
	if totalInfo, ok := info.Data.SpaceInfo["all_total"]; ok {
		usage.Total = fs.NewUsageValue(int64(totalInfo.Size))
	}
	if useInfo, ok := info.Data.SpaceInfo["all_use"]; ok {
		usage.Used = fs.NewUsageValue(int64(useInfo.Size))
	}
	if remainInfo, ok := info.Data.SpaceInfo["all_remain"]; ok {
		usage.Free = fs.NewUsageValue(int64(remainInfo.Size))
	}

	return usage, nil
}

// itemToDirEntry converts a api.File to an fs.DirEntry.
// When the api.File cannot be represented as an fs.DirEntry
// (nil, nil) is returned.
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, item *api.File) (entry fs.DirEntry, err error) {
	switch {
	case item.FID == "": // in case of dir
		// cache the directory ID for later lookups
		f.dirCache.Put(remote, item.CID)
		d := fs.NewDir(remote, time.Time(item.Te)).SetID(item.CID).SetParentID(item.PID)
		return d, nil
	default:
		entry, err = f.newObjectWithInfo(ctx, remote, item)
		if err == fs.ErrorObjectNotFound {
			return nil, nil
		}
		return entry, err
	}
}

func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.File) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		err = o.setMetaData(info)
	} else {
		err = o.readMetaData(ctx)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (f *Fs) readMetaDataForPath(ctx context.Context, path string) (info *api.File, err error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, path, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}

	// checking whether fileObj with name of leaf exists in dirID
	found, err := f.listAll(ctx, dirID, func(item *api.File) bool {
		if item.Name == leaf && item.FID != "" {
			info = item
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrorObjectNotFound
	}
	return info, nil
}

func (f *Fs) getDirID(ctx context.Context, remoteDir string) (int64, error) {
	opts := rest.Opts{
		Method:     http.MethodGet,
		Path:       "/files/getid",
		Parameters: url.Values{},
	}
	opts.Parameters.Set("path", f.opt.Enc.FromStandardPath(remoteDir))

	var err error
	var info api.GetDirIDResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return -1, err
	}
	if !info.State || (remoteDir != "/" && info.CategoryID.String() == "0") {
		fs.Logf(f, "get dir id fail, not state, error: %v, errno: %v, resp: %+v", info.Error, info.Errno, info)
		return -1, fs.ErrorDirNotFound
	}

	cid, _ := info.CategoryID.Int64()
	return cid, nil
}

func (f *Fs) deleteFile(ctx context.Context, fid int64, pid int64) error {
	fs.Logf(f, "delete file, fid: %v, pid: %v", fid, pid)
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/rb/delete",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set("fid[0]", strconv.FormatInt(fid, 10))
	opts.MultipartParams.Set("pid", strconv.FormatInt(pid, 10))
	opts.MultipartParams.Set("ignore_warn", "1")

	var err error
	var info api.BaseResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		if errno, ok := info.Errno.(int64); ok && errno == 990009 {
			time.Sleep(time.Second)
		}
		fs.Logf(f, "delete file fail, not state, err: %v, errno: %v", info.Error, info.Errno)
		return nil
	}

	return nil
}

func (f *Fs) moveFile(ctx context.Context, fid int64, pid int64) error {
	fs.Logf(f, "move file, fid: %v, pid: %v", fid, pid)
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/files/move",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set("fid[0]", strconv.FormatInt(fid, 10))
	opts.MultipartParams.Set("pid", strconv.FormatInt(pid, 10))

	var err error
	var info api.BaseResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		fs.Logf(f, "move file fail, not state")
		return nil
	}

	return nil
}

func (f *Fs) renameFile(ctx context.Context, fid int64, name string) error {
	fs.Logf(f, "rename file, fid: %v, name: %v", fid, name)
	opts := rest.Opts{
		Method:          http.MethodPost,
		Path:            "/files/batch_rename",
		MultipartParams: url.Values{},
	}
	opts.MultipartParams.Set(fmt.Sprintf("files_new_name[%d]", fid), name)

	var err error
	var info api.BaseResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.State {
		fs.Logf(f, "rename file fail, not state, err: %v", info.Error)
		return nil
	}

	return nil
}

func (f *Fs) getURL(ctx context.Context, remote string, pickCode string) (string, error) {
	cacheKey := fmt.Sprintf("url:%s", pickCode)
	if value, ok := f.cache.Get(cacheKey); ok {
		return value.(string), nil
	}

	key := crypto.GenerateKey()
	data, _ := json.Marshal(map[string]string{
		"pickcode": pickCode,
	})

	opts := rest.Opts{
		Method:          http.MethodPost,
		RootURL:         "https://proapi.115.com",
		Path:            "/app/chrome/downurl",
		Parameters:      url.Values{},
		MultipartParams: url.Values{},
	}
	opts.Parameters.Add("t", strconv.FormatInt(time.Now().Unix(), 10))
	opts.MultipartParams.Set("data", crypto.Encode(data, key))
	var err error
	var info api.GetURLResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return "", err
	}

	var encodedData string
	if err := json.Unmarshal(info.Data, &encodedData); err != nil {
		return "", fmt.Errorf("api get download url, call json.Unmarshal fail, body: %s", string(info.Data))
	}
	decodedData, err := crypto.Decode(encodedData, key)
	if err != nil {
		return "", fmt.Errorf("api get download url, call Decode fail, err: %w", err)
	}

	result := api.DownloadData{}
	if err := json.Unmarshal(decodedData, &result); err != nil {
		return "", fmt.Errorf("api get download url, call json.Unmarshal fail, body: %s", string(decodedData))
	}

	for _, info := range result {
		fileSize, _ := info.FileSize.Int64()
		if fileSize == 0 {
			return "", fs.ErrorObjectNotFound
		}
		f.cache.SetDefault(cacheKey, info.URL.URL)
		return info.URL.URL, nil
	}

	return "", fs.ErrorObjectNotFound
}

func (f *Fs) indexInfo(ctx context.Context) (*api.IndexInfoResponse, error) {
	opts := rest.Opts{
		Method: http.MethodGet,
		Path:   "/files/index_info",
	}

	var err error
	var info api.IndexInfoResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (f *Fs) createObject(remote string, modTime time.Time, size int64) *Object {
	return &Object{
		fs:     f,
		remote: remote,
		size:   size,
		mTime:  modTime,
	}
}

func (f *Fs) remotePath(name string) string {
	name = path.Join(f.root, name)
	if name == "" || name[0] != '/' {
		name = "/" + name
	}
	return path.Clean(name)
}

func (f *Fs) flushDir(dir string) {
	cacheKey := fmt.Sprintf("files:%v", path.Clean(dir))
	f.cache.Delete(cacheKey)
}

// ------------------------------------------------------------

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (o *Object) ModTime(ctx context.Context) time.Time {
	err := o.readMetaData(ctx)
	if err != nil {
		fs.Logf(o, "failed to read metadata: %v", err)
		return time.Now()
	}
	return o.mTime
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	err := o.readMetaData(context.TODO())
	if err != nil {
		fs.Logf(o, "failed to read metadata: %v", err)
		return 0
	}
	return o.size
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	return strings.ToLower(o.sha1sum), nil
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// ParentID returns the ID of the Object parent if known, or "" if not
func (o *Object) ParentID() string {
	return o.parent
}

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	o.mTime = modTime
	return nil
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	fs.Logf(o.fs, "open %v, options: %v", o.remote, options)
	targetURL, err := o.fs.getURL(ctx, o.remote, o.pickCode)
	if err != nil {
		return nil, err
	}
	fs.FixRangeOption(options, o.size)
	opts := rest.Opts{
		Method:  http.MethodGet,
		RootURL: targetURL,
		Options: options,
	}

	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, err
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	// info, err := o.fs.readMetaDataForPath(ctx, o.fs.remotePath(o.Remote()))
	// if err != nil {
	// 	fs.Errorf(o.fs, "remove object fail, err: %v, remote: %v", err, o.remote)
	// 	return err
	// }

	// if info.IsDir() {
	// 	return fs.ErrorIsDir
	// }

	// err = o.fs.deleteFile(ctx, info.GetFileID(), info.GetCategoryID())
	// if err != nil {
	// 	return err
	// }

	// parent, _ := path.Split(o.fs.remotePath(o.remote))
	// o.fs.flushDir(parent)

	return nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// TODO: enable file upload
	return fs.ErrorNotImplemented
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.File) error {
	if info.FID == "" {
		return fs.ErrorIsDir
	}
	o.hasMetaData = true
	o.id = info.FID
	o.parent = info.CID
	o.size = info.Size
	o.sha1sum = strings.ToLower(info.Sha)
	o.pickCode = info.PickCode
	o.mTime = time.Time(info.Te)
	o.cTime = time.Time(info.Tp)
	o.aTime = time.Time(info.To)
	return nil
}

// readMetaData gets the metadata if it hasn't already been fetched
func (o *Object) readMetaData(ctx context.Context) error {
	if o.hasMetaData {
		return nil
	}

	info, err := o.fs.readMetaDataForPath(ctx, o.remote)
	if err != nil {
		return err
	}

	return o.setMetaData(info)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Purger = (*Fs)(nil)
	// _ fs.Copier       = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	// _ fs.PublicLinker = (*Fs)(nil)
	// _ fs.CleanUpper   = (*Fs)(nil)
	_ fs.Abouter    = (*Fs)(nil)
	_ fs.Object     = (*Object)(nil)
	_ fs.ObjectInfo = (*Object)(nil)
	_ fs.IDer       = (*Object)(nil)
	_ fs.ParentIDer = (*Object)(nil)
	// _ fs.MimeTyper = (*Object)(nil)
)
