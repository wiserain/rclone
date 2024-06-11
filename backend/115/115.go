package _115

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

// Constants
const (
	domain           = "www.115.com"
	rootURL          = "https://webapi.115.com"
	appVer           = "2.0.3.6"
	defaultUserAgent = "Mozilla/5.0 115Desktop/" + appVer

	minSleep      = 150 * time.Millisecond // 6.67 transactions per second
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential

	maxUploadSize       = 123480309760                   // 115 GiB from https://proapi.115.com/app/uploadinfo
	maxUploadParts      = 10000                          // Part number must be an integer between 1 and 10000, inclusive.
	minChunkSize        = fs.SizeSuffix(1024 * 1024 * 5) // Part size should be in [100KB, 5GB]
	defaultUploadCutoff = fs.SizeSuffix(200 * 1024 * 1024)
	maxUploadCutoff     = fs.SizeSuffix(5 * 1024 * 1024 * 1024)
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "115",
		Description: "115 drive",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
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
			Name:      "cookie",
			Help:      "cookie including UID, CID, SEID",
			Required:  true,
			Sensitive: true,
		}, {
			// this is useless at the moment because there's no way to upload
			// without defaultUserAgent and appVer 2.0.3.6
			Name:     "user_agent",
			Default:  defaultUserAgent,
			Advanced: true,
			Hide:     fs.OptionHideBoth,
			Help: fmt.Sprintf(`HTTP user agent used for 115.

Defaults to "%s" or "--115-user-agent" provided on command line.`, defaultUserAgent),
		}, {
			Name:     "no_check_certificate",
			Default:  true,
			Advanced: true,
			Hide:     fs.OptionHideBoth,
			Help:     `Do not verify the server SSL certificate for 115 (insecure)`,
		}, {
			Name: "root_folder_id",
			Help: `ID of the root folder.
Leave blank normally.

Fill in for rclone to use a non root folder as its starting point.
`,
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:     "hash_memory_limit",
			Help:     "Files bigger than this will be cached on disk to calculate hash if required.",
			Default:  fs.SizeSuffix(10 * 1024 * 1024),
			Advanced: true,
		}, {
			Name: "upload_cutoff",
			Help: `Cutoff for switching to chunked upload.

Any files larger than this will be uploaded in chunks of chunk_size.
The minimum is 0 and the maximum is 5 GiB.`,
			Default:  defaultUploadCutoff,
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: `Chunk size to use for uploading.

When uploading files larger than upload_cutoff they will be uploaded 
as multipart uploads using this chunk size.

Note that "--115-upload-concurrency" chunks of this size are buffered
in memory per transfer.

If you are transferring large files over high-speed links and you have
enough memory, then increasing this will speed up the transfers.

Rclone will automatically increase the chunk size when uploading a
large file of known size to stay below the 10,000 chunks limit.

Increasing the chunk size decreases the accuracy of the progress
statistics displayed with "-P" flag. Rclone treats chunk as sent when
it's buffered by the OSS SDK, when in fact it may still be uploading.
A bigger chunk size means a bigger OSS SDK buffer and progress
reporting more deviating from the truth.
`,
			Default:  minChunkSize,
			Advanced: true,
		}, {
			Name: "max_upload_parts",
			Help: `Maximum number of parts in a multipart upload.

This option defines the maximum number of multipart chunks to use
when doing a multipart upload.

Rclone will automatically increase the chunk size when uploading a
large file of a known size to stay below this number of chunks limit.
`,
			Default:  maxUploadParts,
			Advanced: true,
		}, {
			Name: "upload_concurrency",
			Help: `Concurrency for multipart uploads and copies.

This is the number of chunks of the same file that are uploaded
concurrently for multipart uploads and copies.

If you are uploading small numbers of large files over high-speed links
and these uploads do not fully utilize your bandwidth, then increasing
this may help to speed up the transfers.`,
			Default:  4,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.EncodeLtGt |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLeftSpace |
				encoder.EncodeLeftSpace |
				encoder.EncodeLeftTilde |
				encoder.EncodeCtl |
				encoder.EncodeLeftPeriod |
				encoder.EncodeRightSpace |
				encoder.EncodeRightPeriod |
				encoder.EncodeInvalidUtf8), // 文件名不能包含以下任意字符之一""<>" (20003)
		}},
	})
}

// Options defines the configguration of this backend
type Options struct {
	UID                 string               `config:"uid"`
	CID                 string               `config:"cid"`
	SEID                string               `config:"seid"`
	Cookie              string               `config:"cookie"`
	UserAgent           string               `config:"user_agent"`
	NoCheckCertificate  bool                 `config:"no_check_certificate"`
	RootFolderID        string               `config:"root_folder_id"`
	HashMemoryThreshold fs.SizeSuffix        `config:"hash_memory_limit"`
	UploadCutoff        fs.SizeSuffix        `config:"upload_cutoff"`
	ChunkSize           fs.SizeSuffix        `config:"chunk_size"`
	MaxUploadParts      int                  `config:"max_upload_parts"`
	UploadConcurrency   int                  `config:"upload_concurrency"`
	Enc                 encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote 115 drive
type Fs struct {
	name         string
	root         string
	opt          Options
	features     *fs.Features
	client       *http.Client // authorized client
	srv          *rest.Client
	dirCache     *dircache.DirCache // Map of directory path to directory id
	pacer        *fs.Pacer
	rootFolderID string
	userID       string     // for uploads
	userkey      string     // for uploads
	fileObj      *fs.Object // mod
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
	mTime       time.Time        // modified/updated at
	cTime       time.Time        // created at
	aTime       time.Time        // last accessed at
	durl        *api.DownloadURL // link to download the object
	durlMu      *sync.Mutex
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
func shouldRetry(ctx context.Context, resp *http.Response, info interface{}, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if err == nil {
		switch apiInfo := info.(type) {
		case *api.Base:
			if !apiInfo.State && apiInfo.Errno == 990009 {
				time.Sleep(time.Second)
				// 删除[subdir]操作尚未执行完成，请稍后再试！ (990009)
				return true, fserrors.RetryErrorf("API State false: %s (%d)", apiInfo.Error, apiInfo.Errno)
			} else if !apiInfo.State && apiInfo.Errno == 50038 {
				// can't download: API State false:  (50038)
				return true, fserrors.RetryErrorf("API State false: %s (%d)", apiInfo.Error, apiInfo.Errno)
			}
		}
		return false, nil
	}
	if fserrors.ShouldRetry(err) {
		return true, err
	}
	authRetry := false

	switch apiErr := err.(type) {
	case *api.Error:
		if apiErr.Status == 403 {
			if apiErr.Message == "no cookie" {
				return false, fserrors.FatalError(err)
			}
			time.Sleep(time.Second)
			return true, err
		}
	}

	return authRetry || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// ------------------------------------------------------------

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	// Decode error response
	errResponse := new(api.Error)
	err := rest.DecodeJSON(resp, &errResponse)
	if err != nil {
		fs.Debugf(nil, "Couldn't decode error response: %v", err)
	}
	if errResponse.Message == "" {
		errResponse.Message = resp.Status
	}
	if errResponse.Status == 0 {
		errResponse.Status = resp.StatusCode
	}
	return errResponse
}

// getClient makes an http client according to the options
func getClient(ctx context.Context, opt *Options) *http.Client {
	t := fshttp.NewTransportCustom(ctx, func(t *http.Transport) {
		t.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: opt.NoCheckCertificate,
		}
	})
	return &http.Client{
		Transport: t,
	}
}

// newClientWithPacer sets a new http/rest client with a pacer to Fs
func (f *Fs) newClientWithPacer(ctx context.Context, opt *Options) (err error) {
	// Override few config settings and create a client
	newCtx, ci := fs.AddConfig(ctx)
	ci.UserAgent = opt.UserAgent
	f.client = getClient(newCtx, opt)

	f.srv = rest.NewClient(f.client).SetRoot(rootURL).SetErrorHandler(errorHandler)

	// UID, CID, SEID from cookie
	if opt.Cookie != "" {
		if items := strings.Split(opt.Cookie, ";"); len(items) > 2 {
			for _, item := range items {
				kv := strings.Split(strings.TrimSpace(item), "=")
				if len(kv) != 2 {
					continue
				}
				key := strings.TrimSpace(strings.ToUpper(kv[0]))
				val := strings.TrimSpace(kv[1])
				switch key {
				case "UID", "CID", "SEID":
					reflect.ValueOf(opt).Elem().FieldByName(key).SetString(val)
				}
			}
		}
	}
	f.srv.SetCookie(&http.Cookie{
		Name:     "UID",
		Value:    opt.UID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "CID",
		Value:    opt.CID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	}, &http.Cookie{
		Name:     "SEID",
		Value:    opt.SEID,
		Domain:   domain,
		Path:     "/",
		HttpOnly: true,
	})
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	return nil
}

func checkUploadChunkSize(cs fs.SizeSuffix) error {
	if cs < minChunkSize {
		return fmt.Errorf("%s is less than %s", cs, minChunkSize)
	}
	return nil
}

func checkUploadCutoff(cs fs.SizeSuffix) error {
	if cs > maxUploadCutoff {
		return fmt.Errorf("%s is greater than %s", cs, maxUploadCutoff)
	}
	return nil
}

// newFs partially constructs Fs from the path
//
// It constructs a valid Fs but doesn't attempt to figure out whether
// it is a file or a directory.
func newFs(ctx context.Context, name, path string, m configmap.Mapper) (*Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	err = checkUploadChunkSize(opt.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("115: chunk size: %w", err)
	}
	err = checkUploadCutoff(opt.UploadCutoff)
	if err != nil {
		return nil, fmt.Errorf("115: upload cutoff: %w", err)
	}

	// mod - override rootID from path remote:{ID}
	if rootID, _ := parseRootID(path); len(rootID) > 6 {
		name += rootID
		path = path[strings.Index(path, "}")+1:]
	}

	root := strings.Trim(path, "/")

	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
	}
	f.features = (&fs.Features{
		DuplicateFiles:          false, // duplicatefiles are only possible via web
		CanHaveEmptyDirectories: true,  // can have empty directories
		NoMultiThreading:        true,  // set if can't have multiplethreads on one download open
	}).Fill(ctx, f)

	if err := f.newClientWithPacer(ctx, opt); err != nil {
		return nil, err
	}

	return f, nil
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}

	// mod - parse object id from path remote:{ID}
	var srcFile *api.File
	if rootID, _ := parseRootID(root); len(rootID) > 6 {
		f.opt.RootFolderID = rootID

		srcFile, err = f.getFile(ctx, rootID)
		if err != nil {
			return nil, fmt.Errorf("115: failed checking filetype: %w", err)
		}
		if !srcFile.IsDir() {
			fs.Debugf(nil, "Root ID (File): %s", rootID)
		} else {
			// assume it is a file
			fs.Debugf(nil, "Root ID (Folder): %s", rootID)
			f.opt.RootFolderID = rootID
			srcFile = nil
		}
	}

	// Set the root folder ID
	if f.opt.RootFolderID != "" {
		// use root_folder ID if set
		f.rootFolderID = f.opt.RootFolderID
	} else {
		f.rootFolderID = "0" //根目录 = root directory
	}

	f.dirCache = dircache.New(f.root, f.rootFolderID, f)

	// mod - in case parsed rootID is pointing to a file
	if srcFile != nil {
		tempF := *f
		newRoot := ""
		tempF.dirCache = dircache.New(newRoot, f.rootFolderID, &tempF)
		tempF.root = newRoot
		f.dirCache = tempF.dirCache
		f.root = tempF.root

		obj, _ := f.newObjectWithInfo(ctx, srcFile.Name, srcFile)
		f.root = "isFile:" + srcFile.Name
		f.fileObj = &obj
		return f, fs.ErrorIsFile
	}

	// warm up dircache
	if f.rootFolderID == "0" {
		if rootid, err := f.getDirID(ctx, f.root); err == nil {
			stats, err := f.getStats(ctx, rootid)
			if err != nil {
				return nil, fmt.Errorf("failed to get file stats: %w", err)
			}
			dirPath := ""
			for _, parent := range stats.Paths[1:] {
				dirPath = path.Join(dirPath, parent.FileName)
				f.dirCache.Put(dirPath, parent.FileID.String())
			}
			dirPath = path.Join(dirPath, stats.FileName)
			f.dirCache.Put(dirPath, rootid)
		}
	}

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
	return fs.ModTimeNotSupported
	// meaning that the modification times from the backend shouldn't be used for syncing
	// as they can't be set.
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
	// mod - in case parsed rootID is pointing to a file
	if f.fileObj != nil {
		return *f.fileObj, nil
	}
	return f.newObjectWithInfo(ctx, remote, nil)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	// Find the leaf in pathID
	found, err = f.listAll(ctx, pathID, func(item *api.File) bool {
		if item.Name == leaf && item.IsDir() {
			pathIDOut = item.ID()
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
	existingObj, err := f.NewObject(ctx, src.Remote())
	switch err {
	case nil:
		return existingObj, existingObj.Update(ctx, in, src, options...)
	case fs.ErrorObjectNotFound:
		// Not found so create it
		return f.PutUnchecked(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// putUnchecked uploads the object with the given name and size
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) putUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote string, options ...fs.OpenOption) (fs.Object, error) {
	// upload src with the name of remote
	newObj, err := f.upload(ctx, in, src, remote, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to upload: %w", err)
	}
	o := newObj.(*Object)

	var info *api.File
	found, err := f.listAll(ctx, o.parent, func(item *api.File) bool {
		if strings.ToLower(item.Sha) == o.sha1sum && !item.IsDir() {
			info = item
			return true
		}
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("failed to located updated file: %w", err)
	}
	if !found {
		return nil, fs.ErrorObjectNotFound
	}
	return o, o.setMetaData(info)
}

// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.putUnchecked(ctx, in, src, src.Remote(), options...)
}

// MergeDirs merges the contents of all the directories passed
// in into the first one and rmdirs the other directories.
func (f *Fs) MergeDirs(ctx context.Context, dirs []fs.Directory) (err error) {
	if len(dirs) < 2 {
		return nil
	}
	dstDir := dirs[0]
	for _, srcDir := range dirs[1:] {
		// list the objects
		var IDs []string
		_, err = f.listAll(ctx, srcDir.ID(), func(item *api.File) bool {
			fs.Infof(srcDir, "listing for merging %q", item.Name)
			IDs = append(IDs, item.ID())
			// API doesn't allow to move a large number of objects at once, so doing it in chunked
			if len(IDs) >= ListLimit {
				if err = f.moveFiles(ctx, IDs, dstDir.ID()); err != nil {
					return true
				}
				IDs = nil
			}
			return false
		})
		if err != nil {
			return fmt.Errorf("MergeDirs list failed on %v: %w", srcDir, err)
		}
		// move them into place
		if err = f.moveFiles(ctx, IDs, dstDir.ID()); err != nil {
			return fmt.Errorf("MergeDirs move failed in %v: %w", srcDir, err)
		}
	}

	// rmdir (into trash) the now empty source directory
	var IDs []string
	for _, srcDir := range dirs[1:] {
		fs.Infof(srcDir, "removing empty directory")
		IDs = append(IDs, srcDir.ID())
		// API doesn't allow to delete a large number of objects at once, so doing it in chunked
		if len(IDs) >= ListLimit {
			if err = f.deleteFiles(ctx, IDs); err != nil {
				return err
			}
			IDs = nil
		}
	}
	if err := f.deleteFiles(ctx, IDs); err != nil {
		return fmt.Errorf("MergeDirs failed to rmdir: %w", err)
	}
	return nil
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
	if src.Fs().Name() != f.Name() {
		return nil, fs.ErrorCantMove
	}

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}
	err := srcObj.readMetaData(ctx)
	if err != nil {
		return nil, err
	}

	srcLeaf, srcParentID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}

	// Create temporary object
	dstObj, dstLeaf, dstParentID, err := f.createObject(ctx, remote, srcObj.mTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	if srcParentID != dstParentID {
		// Do the move
		if err = f.moveFiles(ctx, []string{srcObj.id}, dstParentID); err != nil {
			return nil, err
		}
	}
	dstObj.id = srcObj.id

	if srcLeaf != dstLeaf {
		// Rename
		err = f.renameFile(ctx, srcObj.id, dstLeaf)
		if err != nil {
			return nil, fmt.Errorf("move: couldn't rename moved file: %w", err)
		}
	}
	return dstObj, dstObj.readMetaData(ctx)
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

	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcID, srcParentID, srcLeaf, dstParentID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	if srcParentID != dstParentID {
		// Do the move
		err = f.moveFiles(ctx, []string{srcID}, dstParentID)
		if err != nil {
			return fmt.Errorf("couldn't dir move: %w", err)
		}
	}

	// Can't copy and change name in one step so we have to check if we have
	// the correct name after copy
	if srcLeaf != dstLeaf {
		err = f.renameFile(ctx, srcID, dstLeaf)
		if err != nil {
			return fmt.Errorf("dirmove: couldn't rename moved dir: %w", err)
		}
	}
	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	err := srcObj.readMetaData(ctx)
	if err != nil {
		return nil, err
	}

	// Create temporary object
	dstObj, dstLeaf, dstParentID, err := f.createObject(ctx, remote, srcObj.mTime, srcObj.size)
	if err != nil {
		return nil, err
	}
	if srcObj.parent == dstParentID {
		// api restriction
		fs.Debugf(src, "Can't copy - same parent")
		return nil, fs.ErrorCantCopy
	}
	// Copy the object
	if err := f.copyFiles(ctx, []string{srcObj.id}, dstParentID); err != nil {
		return nil, fmt.Errorf("couldn't copy file: %w", err)
	}

	// Can't copy and change name in one step so we have to check if we have
	// the correct name after copy
	srcLeaf, _, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}

	if srcLeaf != dstLeaf {
		// Rename
		err = f.renameFile(ctx, dstObj.id, dstLeaf)
		if err != nil {
			return nil, fmt.Errorf("copy: couldn't rename copied file: %w", err)
		}
	}
	return dstObj, dstObj.readMetaData(ctx)
}

// purgeCheck removes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	rootID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	if check {
		found, err := f.listAll(ctx, rootID, func(item *api.File) bool {
			fs.Debugf(dir, "Rmdir: contains file: %q", item.Name)
			return true
		})
		if err != nil {
			return err
		}
		if found {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	if root != "" {
		err = f.deleteFiles(ctx, []string{rootID})
		if err != nil {
			return err
		}
	} else if check {
		return errors.New("can't purge root directory")
	}
	f.dirCache.FlushDir(dir)
	if err != nil {
		return err
	}
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, true)
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, false)
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	info, err := f.indexInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get usage info: %w", err)
	}

	usage := &fs.Usage{}
	if totalInfo, ok := info.SpaceInfo["all_total"]; ok {
		usage.Total = fs.NewUsageValue(int64(totalInfo.Size))
	}
	if useInfo, ok := info.SpaceInfo["all_use"]; ok {
		usage.Used = fs.NewUsageValue(int64(useInfo.Size))
	}
	if remainInfo, ok := info.SpaceInfo["all_remain"]; ok {
		usage.Free = fs.NewUsageValue(int64(remainInfo.Size))
	}

	return usage, nil
}

// itemToDirEntry converts a api.File to an fs.DirEntry.
// When the api.File cannot be represented as an fs.DirEntry
// (nil, nil) is returned.
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, item *api.File) (entry fs.DirEntry, err error) {
	switch {
	case item.IsDir(): // in case of dir
		// cache the directory ID for later lookups
		f.dirCache.Put(remote, item.ID())
		d := fs.NewDir(remote, time.Time(item.Te)).SetID(item.ID()).SetParentID(item.PID)
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
		durlMu: new(sync.Mutex),
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

// If there is a quicker way of finding a given file than listing the parent, then this readMetaDataForPath should implement it. It makes quite a difference to performance if such a call is available.
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
		if item.Name == leaf && !item.IsDir() {
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

// Creates from the parameters passed in a half finished Object which
// must have setMetaData called on it
//
// Returns the object, leaf, dirID and error.
//
// Used to create new objects
func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (o *Object, leaf string, dirID string, err error) {
	// Create the directory for the object if it doesn't exist
	leaf, dirID, err = f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return
	}
	// Temporary Object under construction
	o = &Object{
		fs:     f,
		remote: remote,
		parent: dirID,
		size:   size,
		mTime:  modTime,
		durlMu: new(sync.Mutex),
	}
	return o, leaf, dirID, nil
}

// ------------------------------------------------------------

var commandHelp = []fs.CommandHelp{{
	Name:  "addurls",
	Short: "Add offline download task for urls",
	Long: `This command adds offline download task for urls.

Usage:

    rclone backend addurls 115:dirpath url1 url2

By default, downloads are saved to the folder "dirpath". 
If this folder doesn't exist or you don't have permission 
to access it, the download will be saved to the default folder 
named "云下载".

This command always exits with code 0, regardless of success.
Instead, check the output (stdout) for any error messages.
`,
}, { // mod
	Name:  "getid",
	Short: "Get an ID of a file or directory",
	Long: `This command is to obtain an ID of a file or directory.

Usage:

    rclone backend getid 115:path {subpath}

The 'path' should point to a directory not a file. Use an extra argument
'subpath' to get an ID of a file located in '115:path'.
`,
}}

// Command the backend to run a named command
//
// The command run is name
// args may be used to read arguments from
// opts may be used to read optional arguments from
//
// The result should be capable of being JSON encoded
// If it is a string or a []string it will be shown to the user
// otherwise it will be JSON encoded and shown to the user like that
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out interface{}, err error) {
	switch name {
	case "addurls":
		return f.addURLs(ctx, "", arg)
	case "getid":
		// mod
		path := ""
		if len(arg) > 0 {
			path = arg[0]
		}
		return f.getID(ctx, path)
	default:
		return nil, fs.ErrorCommandNotFound
	}
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
	return fs.ErrorCantSetModTime
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	if o.id == "" {
		return nil, errors.New("can't download: no id")
	}
	if o.size == 0 {
		// this shouldn't happen as no zero-byte files allowed by api but just in case
		return io.NopCloser(bytes.NewBuffer([]byte(nil))), nil
	}
	if err = o.setDownloadURL(ctx); err != nil {
		return nil, fmt.Errorf("can't download: %w", err)
	}
	fs.FixRangeOption(options, o.size)
	opts := rest.Opts{
		Method:       "GET",
		RootURL:      o.durl.URL,
		Options:      options,
		ExtraHeaders: map[string]string{"Cookie": o.durl.Cookie()},
	}
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, nil, err)
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, err
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteFiles(ctx, []string{o.id})
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("refusing to update with unknown size")
	}

	// upload with new size but old name
	newObj, err := o.fs.putUnchecked(ctx, in, src, o.Remote(), options...)
	if err != nil {
		return err
	}

	// updating object with the same contents(sha1) simply updates some attributes
	// rather than creating a new one. So we shouldn't delete old object
	newO := newObj.(*Object)
	if !(newO.id == o.id && newO.pickCode == o.pickCode && newO.sha1sum == o.sha1sum) {
		// Delete duplicate after successful upload
		if err = o.Remove(ctx); err != nil {
			return fmt.Errorf("failed to remove old version: %w", err)
		}
	}

	// Replace guts of old object with new one
	*o = *newO

	return nil
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.File) error {
	if info.IsDir() {
		return fs.ErrorIsDir
	}
	o.hasMetaData = true
	o.id = info.ID()
	o.parent = info.CID
	o.size = info.Size
	o.sha1sum = strings.ToLower(info.Sha)
	o.pickCode = info.PickCode
	o.mTime = time.Time(info.Te)
	o.cTime = time.Time(info.Tp)
	o.aTime = time.Time(info.To)
	return nil
}

// setDownloadURL ensures a link for opening an object
func (o *Object) setDownloadURL(ctx context.Context) error {
	o.durlMu.Lock()
	defer o.durlMu.Unlock()

	// check if the current link is valid
	if o.durl.Valid() {
		return nil
	}

	downURL, err := o.fs.getDownloadURL(ctx, o.pickCode)
	if err != nil {
		return err
	}
	o.durl = downURL
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
	// _ fs.PublicLinker    = (*Fs)(nil)
	// _ fs.CleanUpper      = (*Fs)(nil)
	// _ fs.UserInfoer      = (*Fs)(nil)
	// _ fs.MimeTyper = (*Object)(nil)
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.MergeDirser     = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Commander       = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.ObjectInfo      = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.ParentIDer      = (*Object)(nil)
)
