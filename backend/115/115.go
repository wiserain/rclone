package _115

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
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
	userAgent = "Mozilla/5.0 115Desktop/2.0.3.6" // TODO: make it configurable
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
			Name:     "hash_memory_limit",
			Help:     "Files bigger than this will be cached on disk to calculate hash if required.",
			Default:  fs.SizeSuffix(10 * 1024 * 1024),
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
	RootFolderID        string               `config:"root_folder_id"`
	HashMemoryThreshold fs.SizeSuffix        `config:"hash_memory_limit"`
	Enc                 encoder.MultiEncoder `config:"encoding"`
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
	// drv          *driver115.Pan115Client
	ui *api.UploadInfo
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
	f.pacer.SetMaxConnections(2) // TODO: tune rate limit
	return nil
}

// func (f *Fs) newDriver115() (err error) {
// 	TlsInsecureSkipVerify := true
// 	opts := []driver115.Option{
// 		driver115.UA(userAgent),
// 		func(c *driver115.Pan115Client) {
// 			c.Client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: TlsInsecureSkipVerify})
// 		},
// 	}
// 	f.drv = driver115.New(opts...)
// 	cr := &driver115.Credential{}
// 	cookie := fmt.Sprintf("UID=%s;CID=%s;SEID=%s", f.opt.UID, f.opt.CID, f.opt.SEID)
// 	if err = cr.FromCookie(cookie); err != nil {
// 		return fmt.Errorf("failed to login by cookies: %w", err)
// 	}
// 	f.drv.ImportCredential(cr)
// 	return f.drv.LoginCheck()
// }

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
		name: name,
		root: root,
		opt:  *opt,
	}
	f.features = (&fs.Features{
		DuplicateFiles:          true, // allows duplicate files
		CanHaveEmptyDirectories: true, // can have empty directories
		NoMultiThreading:        true, // set if can't have multiplethreads on one download open
		// ChunkWriterDoesntSeek    bool // set if the chunk writer doesn't need to read the data more than once
		// TODO: check other features available/possible
	}).Fill(ctx, f)

	if err := f.newClientWithPacer(ctx); err != nil {
		return nil, err
	}
	// if err := f.newDriver115(); err != nil {
	// 	return nil, err
	// }

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
	return fs.ModTimeNotSupported // TODO: check further
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
	fs.Debugf(nil, "Put %s", src)
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
		// newObj := &Object{
		// 	fs:     f,
		// 	remote: src.Remote(),
		// }
		// return newObj, newObj.Update(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// TODO
// // PutStream uploads to the remote path with the modTime given of indeterminate size
// func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
// 	// return f.Put(ctx, in, src, options...)
// 	return nil, fs.ErrorNotImplemented
// }

// putUnchecked uploads the object with the given name and size
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) putUnchecked(ctx context.Context, in io.Reader, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	newObj := &Object{
		fs: f,
		// remote: src.Remote(),
		remote: remote,
		durlMu: new(sync.Mutex),
	}
	if err := newObj.upload(ctx, in, src, options...); err != nil {
		return nil, fmt.Errorf("failed to upload: %w", err)
	}

	var info *api.File
	found, err := f.listAll(ctx, newObj.parent, func(item *api.File) bool {
		if strings.ToLower(item.Sha) == newObj.sha1sum && item.FID != "" {
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
	return newObj, newObj.setMetaData(info)
}

// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.putUnchecked(ctx, in, src.Remote(), src, options...)
}

// TODO
// // MergeDirs merges the contents of all the directories passed
// // in into the first one and rmdirs the other directories.
// func (f *Fs) MergeDirs(ctx context.Context, dirs []fs.Directory) error {
// 	return fs.ErrorNotImplemented
// }

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
		size:   size,
		mTime:  modTime,
		durlMu: new(sync.Mutex),
	}
	return o, leaf, dirID, nil
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
	// o.mTime = modTime
	// return nil
	return fs.ErrorCantSetModTime // TODO check later
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
		return shouldRetry(ctx, resp, err)
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
	fs.Debugf(o, "Update %s", src)
	if src.Size() < 0 {
		return errors.New("refusing to update with unknown size")
	}

	// upload with new size but old name
	info, err := o.fs.putUnchecked(ctx, in, o.Remote(), src, options...)
	// info, err := o.fs.putUnchecked(ctx, in, o.Remote(), src.Size(), options...)
	if err != nil {
		return err
	}

	// Delete duplicate after successful upload
	err = o.Remove(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove old version: %w", err)
	}

	// Replace guts of old object with new one
	*o = *info.(*Object)

	return nil
}

// upload uploads the object with or without using a temporary file name
func (o *Object) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	size := src.Size()
	remote := o.Remote()

	// Create the directory for the object if it doesn't exist
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return err
	}

	// check upload info
	if o.fs.ui == nil {
		// TODO: fixed values?
		ui, err := o.fs.getUploadInfo(ctx)
		if err != nil {
			return fmt.Errorf("failed to get upload info: %w", err)
		}
		o.fs.ui = ui
	}
	if size > o.fs.ui.SizeLimit {
		return fmt.Errorf("file size exceeds the upload limit: %d > %d", size, o.fs.ui.SizeLimit)
	}

	var wrap accounting.WrapFn
	var cleanup func()

	// Calculate sha1sum; grabbed from package jottacloud
	hashStr, err := src.Hash(ctx, hash.SHA1)
	if err != nil || hashStr == "" {
		// unwrap the accounting from the input, we use wrap to put it
		// back on after the buffering
		in, wrap = accounting.UnWrap(in)
		hashStr, in, cleanup, err = bufferIOwithSHA1(in, size, int64(o.fs.opt.HashMemoryThreshold))
		defer cleanup()
		if err != nil {
			return fmt.Errorf("failed to calculate SHA1: %w", err)
		}
	}

	o.sha1sum = strings.ToLower(hashStr)
	o.parent = dirID

	var uii *api.UploadInitInfo
	signKey, signVal := "", ""
	err = o.fs.pacer.Call(func() (bool, error) {
		uii, err = o.fs.initUpload(ctx, size, leaf, dirID, hashStr, signKey, signVal)
		if err != nil {
			return false, err
		}
		if uii.Status == 7 {
			if wrap == nil {
				in, wrap = accounting.UnWrap(in)
				in, cleanup, err = bufferIO(in, size, int64(o.fs.opt.HashMemoryThreshold))
				defer cleanup()
				if err != nil {
					return false, fmt.Errorf("failed to buffer io: %w", err)
				}
			}
			signKey = uii.SignKey
			signVal, err = calcBlockSHA1(in, uii.SignCheck)
			if err != nil {
				return false, fmt.Errorf("failed to calculate block hash: %w", err)
			}
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return
	}
	switch uii.Status {
	case 1:
		fs.Debugf(o, "Starting upload...")
	case 2:
		fs.Debugf(o, "Uploaded by hash") // match by hash; no outbound traffic
		return
	default:
		return fmt.Errorf("unexpected status: %#v", uii)
	}

	// Wrap the accounting back onto the stream
	// if wrap != nil {
	// 	in = wrap(in)
	// }

	_ = options // TODO: pass options to uploader
	return o.fs.uploadSinglepart(ctx, uii, in)
	// if size <= int64(fs.Kibi) { // 文件大小小于1KB，改用普通模式上传
	// 	return o.fs.uploadSinglepart(ctx, uii, in)
	// }
	// if wrap == nil {
	// 	in, wrap = accounting.UnWrap(in)
	// 	in, cleanup, err = bufferIO(in, size, int64(o.fs.opt.HashMemoryThreshold))
	// 	defer cleanup()
	// 	if err != nil {
	// 		return fmt.Errorf("failed to buffer io: %w", err)
	// 	}
	// 	in = wrap(in)
	// }
	// return o.fs.uploadMultipart(ctx, uii, size, in)
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

// setDownloadURL ensures a link for opening an object
func (o *Object) setDownloadURL(ctx context.Context) error {
	o.durlMu.Lock()
	defer o.durlMu.Unlock()

	// check if the current link is valid
	if o.durl.Valid() {
		return nil
	}

	downURL, err := o.fs.getDownURL(ctx, o.pickCode, "") // TODO: UA matter?
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
	// _ fs.Commander       = (*Fs)(nil)
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
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.ObjectInfo      = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.ParentIDer      = (*Object)(nil)
)
