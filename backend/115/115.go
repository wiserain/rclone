package _115

// ------------------------------------------------------------
// NOTE
// ------------------------------------------------------------

// maximum concurrent downloads
// `--transfers=10 --multi-thread-streams=1` or `--transfers=1 --multi-thread-streams=2`

// Potential error when performing file operations like move, copy, delete, and addurls
// for a large number of files. Maybe we should make API calls in chunked.

// ------------------------------------------------------------
// TODO
// ------------------------------------------------------------

// * Implement rclone cleanup command - where is an API for emptying trash?
// * Implement rclone config userinfo - doesn't seem to have useful info https://my.115.com/?ct=ajax&ac=nav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/backend/115/dircache"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

// Constants
const (
	domain           = "www.115.com"
	rootURL          = "https://webapi.115.com"
	defaultUserAgent = "Mozilla/5.0 115Browser/27.0.7.5"

	defaultMinSleep = fs.Duration(1000 * time.Millisecond) // 1 transactions per second
	maxSleep        = 2 * time.Second
	decayConstant   = 2 // bigger for slower decay, exponential

	defaultConTimeout = fs.Duration(10 * time.Second) // from rclone global flags - Connect timeout (default 1m0s)
	defaultTimeout    = fs.Duration(45 * time.Second) // from rclone global flags - IO idle timeout (default 5m0s)

	maxUploadSize       = 123480309760                   // 115 GiB from https://proapi.115.com/app/uploadinfo
	maxUploadParts      = 10000                          // Part number must be an integer between 1 and 10000, inclusive.
	minChunkSize        = fs.SizeSuffix(1024 * 1024 * 5) // Part size should be in [100KB, 5GB]
	defaultUploadCutoff = fs.SizeSuffix(200 * 1024 * 1024)
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
			Help:      "UID from cookie (deprecated, consider migrating to option cookie)",
			Required:  true,
			Sensitive: true,
			Hide:      fs.OptionHideBoth,
		}, {
			Name:      "cid",
			Help:      "CID from cookie (deprecated, consider migrating to option cookie)",
			Required:  true,
			Sensitive: true,
			Hide:      fs.OptionHideBoth,
		}, {
			Name:      "seid",
			Help:      "SEID from cookie (deprecated, consider migrating to option cookie)",
			Required:  true,
			Sensitive: true,
			Hide:      fs.OptionHideBoth,
		}, {
			Name:      "kid",
			Help:      "KID from cookie (deprecated, consider migrating to option cookie)",
			Required:  true,
			Sensitive: true,
			Hide:      fs.OptionHideBoth,
		}, {
			Name: "cookie",
			Help: `Provide a cookie in the format "UID=...; CID=...; SEID=...; KID=...;".

This setting takes precedence over any individually defined UID, CID, SEID or KID options.
Additionally, you can provide a comma-separated list of cookies to distribute requests 
across multiple client instances for load balancing.`,
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "share_code",
			Help:      "share code from share link",
			Sensitive: true,
		}, {
			Name:      "receive_code",
			Help:      "password from share link",
			Sensitive: true,
		}, {
			Name:     "user_agent",
			Default:  defaultUserAgent,
			Advanced: true,
			Help: fmt.Sprintf(`HTTP user agent used for 115.

Defaults to "%s" or "--115-user-agent" provided on command line.`, defaultUserAgent),
		}, {
			Name: "root_folder_id",
			Help: `ID of the root folder.
Leave blank normally.

Fill in for rclone to use a non root folder as its starting point.
`,
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:     "list_chunk",
			Default:  115,
			Help:     "Size of listing chunk. Max: 1150",
			Advanced: true,
		}, {
			Name:     "pacer_min_sleep",
			Default:  defaultMinSleep,
			Help:     "Minimum time to sleep between API calls.",
			Advanced: true,
		}, {
			Name:     "contimeout",
			Default:  defaultConTimeout,
			Help:     "Connect timeout.",
			Advanced: true,
		}, {
			Name:     "timeout",
			Default:  defaultTimeout,
			Help:     "IO idle timeout.",
			Advanced: true,
		}, {
			Name:     "upload_hash_only",
			Default:  false,
			Advanced: true,
			Help: `Skip uploading files that require network traffic, including

	1) Incoming traffic for calculating file hashes locally
	2) Outgoing traffic for uploading to the storage server`,
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
			Default:  1,
			Advanced: true,
		}, {
			Name:      "download_cookie",
			Sensitive: true,
			Advanced:  true,
			Help: `Set a comma-separated list of cookies for the download-only clients. 

This enables separate client instances dedicated to downloading files`,
		}, {
			Name:     "download_no_proxy",
			Default:  false,
			Advanced: true,
			Help: `Disable proxy settings for the download-only client.
			
Use this flag with the "--115-download-cookie" option to bypass proxy settings for downloads.`,
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
	KID                 string               `config:"kid"`
	Cookie              string               `config:"cookie"`
	ShareCode           string               `config:"share_code"`
	ReceiveCode         string               `config:"receive_code"`
	UserAgent           string               `config:"user_agent"`
	RootFolderID        string               `config:"root_folder_id"`
	ListChunk           int                  `config:"list_chunk"`
	PacerMinSleep       fs.Duration          `config:"pacer_min_sleep"`
	ConTimeout          fs.Duration          `config:"contimeout"`
	Timeout             fs.Duration          `config:"timeout"`
	HashMemoryThreshold fs.SizeSuffix        `config:"hash_memory_limit"`
	UploadHashOnly      bool                 `config:"upload_hash_only"`
	UploadCutoff        fs.SizeSuffix        `config:"upload_cutoff"`
	ChunkSize           fs.SizeSuffix        `config:"chunk_size"`
	MaxUploadParts      int                  `config:"max_upload_parts"`
	UploadConcurrency   int                  `config:"upload_concurrency"`
	DownloadCookie      string               `config:"download_cookie"`
	DownloadNoProxy     bool                 `config:"download_no_proxy"`
	Enc                 encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote 115 drive
type Fs struct {
	name         string
	root         string
	opt          Options
	features     *fs.Features
	srv          *poolClient        // authorized client
	dsrv         *poolClient        // download-only client
	dirCache     *dircache.DirCache // Map of directory path to directory id
	pacer        *fs.Pacer
	rootFolder   string // path of the absolute root
	rootFolderID string
	appVer       string     // parsed from user-agent; // https://appversion.115.com/1/web/1.0/api/getMultiVer
	userID       string     // for uploads, adding offline tasks, and receiving from share link
	userkey      string     // lazy-loaded as it's only for uploads
	isShare      bool       // mark it is from shared or not
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
	modTime     time.Time        // modification time of the object
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
				return true, fserrors.RetryErrorf("API Error: %s (%d)", apiInfo.Error, apiInfo.Errno)
			} else if !apiInfo.State && apiInfo.Errno == 50038 {
				// can't download: API Error:  (50038)
				return true, fserrors.RetryErrorf("API Error: %s (%d)", apiInfo.Error, apiInfo.Errno)
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

// getCookies extracts UID, CID, SEID and KID from a cookie string and returns of a list of *http.Cookie
func getCookies(cookie string) (cks []*http.Cookie) {
	if cookie == "" {
		return
	}

	items := strings.Split(cookie, ";")
	for _, item := range items {
		kv := strings.Split(strings.TrimSpace(item), "=")
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(strings.ToUpper(kv[0]))
		val := strings.TrimSpace(kv[1])
		switch key {
		case "UID", "CID", "SEID", "KID":
			if val != "" {
				cks = append(cks, &http.Cookie{Name: key, Value: val, Domain: domain, Path: "/", HttpOnly: true})
			}
		}
	}
	return
}

// getClient makes an http client according to the options
func getClient(ctx context.Context, opt *Options) *http.Client {
	t := fshttp.NewTransportCustom(ctx, func(t *http.Transport) {
		t.TLSHandshakeTimeout = time.Duration(opt.ConTimeout)
		t.ResponseHeaderTimeout = time.Duration(opt.Timeout)
		if opt.DownloadCookie != "" && opt.DownloadNoProxy {
			t.Proxy = nil
		}
	})
	return &http.Client{
		Transport: t,
	}
}

// poolClient wraps a pool of rest.Client for load-balancing requests
type poolClient struct {
	clients      []*rest.Client
	clientMu     *sync.Mutex
	currentIndex int
}

func (p *poolClient) client() *rest.Client {
	if len(p.clients) == 1 {
		return p.clients[0]
	}
	p.clientMu.Lock()
	defer p.clientMu.Unlock()
	cli := p.clients[p.currentIndex]
	p.currentIndex = (p.currentIndex + 1) % len(p.clients)
	return cli
}

func (p *poolClient) CallJSON(ctx context.Context, opts *rest.Opts, request interface{}, response interface{}) (resp *http.Response, err error) {
	return p.client().CallJSON(ctx, opts, request, response)
}

func (p *poolClient) Call(ctx context.Context, opts *rest.Opts) (resp *http.Response, err error) {
	return p.client().Call(ctx, opts)
}

func (p *poolClient) Do(req *http.Request) (*http.Response, error) {
	return p.client().Do(req)
}

func newPoolClient(ctx context.Context, opt *Options, cookies string) *poolClient {
	var clients []*rest.Client
	for _, cookie := range strings.Split(cookies, ",") {
		if cks := getCookies(cookie); len(cks) == 4 {
			cli := rest.NewClient(getClient(ctx, opt)).SetRoot(rootURL).SetErrorHandler(errorHandler)
			cli.SetCookie(cks...)
			clients = append(clients, cli)
		}
	}
	if len(clients) == 0 {
		return nil
	}
	return &poolClient{
		clients:  clients,
		clientMu: new(sync.Mutex),
	}
}

// newClientWithPacer sets a new pool client with a pacer to Fs
func (f *Fs) newClientWithPacer(ctx context.Context, opt *Options) (err error) {
	// Override few config settings and create a client
	newCtx, ci := fs.AddConfig(ctx)
	ci.UserAgent = opt.UserAgent

	f.srv = newPoolClient(newCtx, opt, opt.Cookie)
	if f.srv == nil {
		// if not found from opt.Cookie
		cookie := fmt.Sprintf("UID=%s;CID=%s;SEID=%s;KID=%s", opt.UID, opt.CID, opt.SEID, opt.KID)
		f.srv = newPoolClient(newCtx, opt, cookie)
	}
	if f.srv == nil {
		return fmt.Errorf("no cookies")
	}

	// download-only clients
	f.dsrv = newPoolClient(newCtx, opt, opt.DownloadCookie)
	f.userID, _, _ = strings.Cut(opt.UID, "_")
	adjustedMinSleep := time.Duration(opt.PacerMinSleep)
	if numClients := len(f.srv.clients); numClients > 1 {
		adjustedMinSleep /= time.Duration(numClients)
		fs.Debugf(nil, "Starting newFs with %d clients", numClients)
	}
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(adjustedMinSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	return nil
}

func checkUploadChunkSize(cs fs.SizeSuffix) error {
	if cs < minChunkSize {
		return fmt.Errorf("%s is less than %s", cs, minChunkSize)
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

	// mod - override rootID from path remote:{ID}
	if rootID, _, _ := parseRootID(path); rootID != "" {
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
		ServerSideAcrossConfigs: true,  // Can copy from shared FS (this is checked in Copy/Move/DirMove)
	}).Fill(ctx, f)

	// setting appVer
	re := regexp.MustCompile(`\d+\.\d+\.\d+(\.\d+)?$`)
	if m := re.FindStringSubmatch(opt.UserAgent); m == nil {
		return nil, fmt.Errorf("%q does not contain a valid app version", opt.UserAgent)
	} else {
		f.appVer = m[0]
		fs.Debugf(nil, "Using App Version %q from User-Agent %q", f.appVer, opt.UserAgent)
	}

	err = f.newClientWithPacer(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize clients: %w", err)
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
	if rootID, receiveCode, _ := parseRootID(root); len(rootID) == 19 {
		info, err := f.getFile(ctx, rootID, "")
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			// When the parsed `rootID` points to a file,
			// commands requiring listing operations (e.g., `ls*`, `cat`) are not supported
			// `copy` has been verified to work correctly
			f.dirCache = dircache.New("", info.ParentID(), f)
			_ = f.dirCache.FindRoot(ctx, false)
			obj, _ := f.newObjectWithInfo(ctx, info.Name, info)
			f.root = "isFile:" + info.Name
			f.fileObj = &obj
			return f, fs.ErrorIsFile
		}
		f.opt.RootFolderID = rootID
	} else if len(rootID) == 11 {
		f.opt.ShareCode = rootID
		f.opt.ReceiveCode = receiveCode
	}

	// mark it is from shared or not
	f.isShare = f.opt.ShareCode != "" && f.opt.ReceiveCode != ""

	// Set the root folder ID
	if f.isShare {
		// should be empty to let dircache run with forward search
		f.rootFolderID = ""
	} else if f.opt.RootFolderID != "" {
		// use root_folder ID if set
		f.rootFolderID = f.opt.RootFolderID
	} else {
		f.rootFolderID = "0" //根目录 = root directory
	}

	// Set the root folder path if it is not on the absolute root
	if f.rootFolderID != "" && f.rootFolderID != "0" {
		f.rootFolder, err = f.getDirPath(ctx, f.rootFolderID)
		if err != nil {
			return nil, err
		}
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
		tempF.dirCache.Fill(f.dirCache)
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
	// try using getDirID first
	if f.rootFolderID == "0" && !f.isShare {
		if path, ok := f.dirCache.GetInv(pathID); ok {
			if pathIDOut, err = f.getDirID(ctx, path+"/"+leaf); err != nil {
				if err == fs.ErrorDirNotFound {
					return pathIDOut, found, nil
				}
				return
			}
			return pathIDOut, true, err
		}
	}
	// Find the leaf in pathID
	found, err = f.listAll(ctx, pathID, f.opt.ListChunk, false, true, func(item *api.File) bool {
		if item.Name == leaf {
			pathIDOut = item.ID()
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// GetDirID wraps `getDirID` to provide an interface for DirCacher
func (f *Fs) GetDirID(ctx context.Context, dir string) (string, error) {
	return f.getDirID(ctx, f.rootFolder+"/"+dir)
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
	_, err = f.listAll(ctx, dirID, f.opt.ListChunk, false, false, func(item *api.File) bool {
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
	if f.isShare {
		return "", errors.New("unsupported for shared filesystem")
	}
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
	if fs.GetConfig(ctx).NoCheckDest {
		return f.PutUnchecked(ctx, in, src, options...)
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
		if !errors.Is(err, lz4.ErrInvalidSourceShortBuffer) {
			return nil, fmt.Errorf("failed to upload: %w", err)
		}
		// In this case, the upload (perhaps via hash) could be successful,
		/// so let the subsequent process locate the uploaded object.
	}
	o := newObj.(*Object)

	if o.hasMetaData {
		return o, nil
	}

	var info *api.File
	found, err := f.listAll(ctx, o.parent, 32, true, false, func(item *api.File) bool {
		if strings.ToLower(item.Sha) == o.sha1sum {
			info = item
			return true
		}
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("failed to locate updated file: %w", err)
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
	if f.isShare {
		return errors.New("unsupported for shared filesystem")
	}
	if len(dirs) < 2 {
		return nil
	}
	dstDir := dirs[0]
	for _, srcDir := range dirs[1:] {
		// list the objects
		var IDs []string
		_, err = f.listAll(ctx, srcDir.ID(), f.opt.ListChunk, false, false, func(item *api.File) bool {
			fs.Infof(srcDir, "listing for merging %q", item.Name)
			IDs = append(IDs, item.ID())
			// API doesn't allow to move a large number of objects at once, so doing it in chunked
			if len(IDs) >= f.opt.ListChunk {
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
		if len(IDs) >= f.opt.ListChunk {
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
	if f.isShare {
		fs.Debugf(src, "Can't move - shared filesystem")
		return nil, fs.ErrorCantMove
	}
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

	// Create temporary object - still needs id, sha1sum, pickCode
	dstObj, dstLeaf, dstParentID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
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
	dstObj.sha1sum = srcObj.sha1sum
	dstObj.pickCode = srcObj.pickCode
	dstObj.hasMetaData = true

	if srcLeaf != dstLeaf {
		// Rename
		err = f.renameFile(ctx, srcObj.id, dstLeaf)
		if err != nil {
			return nil, fmt.Errorf("move: couldn't rename moved file: %w", err)
		}
	}
	return dstObj, nil
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
	if f.isShare {
		fs.Debugf(src, "Can't move directory - shared filesystem")
		return fs.ErrorCantDirMove
	}
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
	if f.isShare {
		fs.Debugf(src, "Can't copy - shared filesystem")
		return nil, fs.ErrorCantCopy
	}
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	err := srcObj.readMetaData(ctx)
	if err != nil {
		return nil, err
	}

	// Create temporary object - still needs id, sha1sum, pickCode
	dstObj, dstLeaf, dstParentID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}
	if srcObj.parent == dstParentID {
		// api restriction
		fs.Debugf(src, "Can't copy - same parent")
		return nil, fs.ErrorCantCopy
	}
	// Copy the object
	if srcObj.fs.isShare {
		if err := f.copyFromShareSrc(ctx, src, dstParentID); err != nil {
			return nil, fmt.Errorf("couldn't copy from share: %w", err)
		}
	} else {
		if err := f.copyFiles(ctx, []string{srcObj.id}, dstParentID); err != nil {
			return nil, fmt.Errorf("couldn't copy file: %w", err)
		}
	}
	// Update the copied object with new parent but old name
	if info, err := dstObj.fs.readMetaDataForPath(ctx, srcObj.remote); err != nil {
		return nil, fmt.Errorf("copy: couldn't locate copied file: %w", err)
	} else if err = dstObj.setMetaData(info); err != nil {
		return nil, err
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
	if f.isShare {
		return errors.New("unsupported for shared filesystem")
	}
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	rootID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	if check {
		found, err := f.listAll(ctx, rootID, 32, false, false, func(item *api.File) bool {
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
		d := fs.NewDir(remote, item.ModTime()).SetID(item.ID()).SetParentID(item.ParentID())
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
	found, err := f.listAll(ctx, dirID, f.opt.ListChunk, true, false, func(item *api.File) bool {
		if item.Name == leaf {
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
		fs:      f,
		remote:  remote,
		parent:  dirID,
		size:    size,
		modTime: modTime,
		durlMu:  new(sync.Mutex),
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
}, {
	Name:  "addshare",
	Short: "Add shared files/dirs from a share link",
	Long: `This command adds shared files/dirs from a share link.

Usage:

    rclone backend addshare 115:dirpath link

All content from the link will be copied to the folder "dirpath". 
If the path doesn't exist, rclone will create it for you.
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
	if f.isShare {
		return nil, errors.New("unsupported for shared filesystem")
	}
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
	case "addshare":
		if len(arg) < 1 {
			return nil, errors.New("need at least 1 argument")
		}
		shareCode, receiveCode, err := parseShareLink(arg[0])
		if err != nil {
			return nil, err
		}
		dirID, err := f.dirCache.FindDir(ctx, "", true)
		if err != nil {
			return nil, err
		}
		return nil, f.copyFromShare(ctx, shareCode, receiveCode, "", dirID)
	case "stats": // 显示属性
		path := ""
		if len(arg) > 0 {
			path = arg[0]
		}
		cid, err := f.getID(ctx, path)
		if err != nil {
			return nil, err
		}
		return f.getStats(ctx, cid)
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
	return o.modTime
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

// open a url for reading
func (o *Object) open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", o.durl.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Cookie", o.durl.Cookie())
	fs.FixRangeOption(options, o.size)
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	if o.size == 0 {
		// Don't supply range requests for 0 length objects as they always fail
		delete(req.Header, "Range")
	}
	var res *http.Response
	srv := o.fs.srv
	if o.fs.dsrv != nil {
		srv = o.fs.dsrv
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		res, err = srv.Do(req)
		return shouldRetry(ctx, res, nil, err)
	})
	if err != nil {
		return nil, fmt.Errorf("open file failed: %w", err)
	}
	return res.Body, nil
}

// Open opens the file for read. Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	if o.id == "" {
		return nil, errors.New("can't download: no id")
	}
	if o.size == 0 {
		// no need to waste a transaction for getting download url
		return io.NopCloser(bytes.NewBuffer([]byte(nil))), nil
	}
	if err = o.setDownloadURL(ctx); err != nil {
		return nil, fmt.Errorf("can't download: %w", err)
	}
	if o.durl.URL == "" {
		return nil, errors.New("can't download: no url")
	}
	return o.open(ctx, options...)
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
	if o.hasMetaData && !(newO.id == o.id && newO.pickCode == o.pickCode && newO.sha1sum == o.sha1sum) {
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
	o.parent = info.ParentID()
	o.size = int64(info.Size)
	o.sha1sum = strings.ToLower(info.Sha)
	o.pickCode = info.PickCode
	o.modTime = info.ModTime()
	return nil
}

// setMetaDataFromCallBack sets the metadata from callback
func (o *Object) setMetaDataFromCallBack(data *api.CallbackData) error {
	// parent, size, sha1sum and modTime are assumed to be set
	o.hasMetaData = true
	o.id = data.FileID
	o.pickCode = data.PickCode
	return nil
}

// setDownloadURL ensures a link for opening an object
func (o *Object) setDownloadURL(ctx context.Context) (err error) {
	o.durlMu.Lock()
	defer o.durlMu.Unlock()

	// check if the current link is valid
	if o.durl.Valid() {
		return
	}

	if o.fs.isShare {
		o.durl, err = o.fs.getDownloadURLFromShare(ctx, o.id)
	} else {
		o.durl, err = o.fs.getDownloadURL(ctx, o.pickCode)
	}
	return
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
