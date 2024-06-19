// Implements multipart uploading for 115. Mostly from lib/multipart
package _115

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/chunksize"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/pool"
	"golang.org/x/sync/errgroup"
)

const (
	bufferSize           = 1024 * 1024     // default size of the pages used in the reader
	bufferCacheSize      = 64              // max number of buffers to keep in cache
	bufferCacheFlushTime = 5 * time.Second // flush the cached buffers after this long
)

// bufferPool is a global pool of buffers
var (
	bufferPool     *pool.Pool
	bufferPoolOnce sync.Once
)

// get a buffer pool
func getPool() *pool.Pool {
	bufferPoolOnce.Do(func() {
		ci := fs.GetConfig(context.Background())
		// Initialise the buffer pool when used
		bufferPool = pool.New(bufferCacheFlushTime, bufferSize, bufferCacheSize, ci.UseMmap)
	})
	return bufferPool
}

// NewRW gets a pool.RW using the multipart pool
func NewRW() *pool.RW {
	return pool.NewRW(getPool())
}

// UploadMultipart does a generic multipart upload from src using f as OpenChunkWriter.
//
// in is read seqentially and chunks from it are uploaded in parallel.
//
// It returns the chunkWriter used in case the caller needs to extract any private info from it.
func (w *ossChunkWriter) Upload(ctx context.Context) (err error) {
	// make concurrency machinery
	concurrency := w.f.opt.UploadConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	tokens := pacer.NewTokenDispenser(concurrency)

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer atexit.OnError(&err, func() {
		cancel()
		fs.Debugf(w.o, "multipart upload: Cancelling...")
		errCancel := w.Abort(ctx)
		if errCancel != nil {
			fs.Debugf(w.o, "multipart upload: failed to cancel: %v", errCancel)
		}
	})()

	var (
		g, gCtx   = errgroup.WithContext(uploadCtx)
		finished  = false
		off       int64
		size      = w.size
		chunkSize = w.chunkSize
	)

	// Do the accounting manually
	in, acc := accounting.UnWrapAccounting(w.in)

	for partNum := int64(0); !finished; partNum++ {
		// Get a block of memory from the pool and token which limits concurrency.
		tokens.Get()
		rw := NewRW()
		if acc != nil {
			rw.SetAccounting(acc.AccountRead)
		}

		free := func() {
			// return the memory and token
			_ = rw.Close() // Can't return an error
			tokens.Put()
		}

		// Fail fast, in case an errgroup managed function returns an error
		// gCtx is cancelled. There is no point in uploading all the other parts.
		if gCtx.Err() != nil {
			free()
			break
		}

		// Read the chunk
		var n int64
		n, err = io.CopyN(rw, in, chunkSize)
		if err == io.EOF {
			if n == 0 && partNum != 0 { // end if no data and if not first chunk
				free()
				break
			}
			finished = true
		} else if err != nil {
			free()
			return fmt.Errorf("multipart upload: failed to read source: %w", err)
		}

		partNum := partNum
		partOff := off
		off += n
		g.Go(func() (err error) {
			defer free()
			fs.Debugf(w.o, "multipart upload: starting chunk %d size %v offset %v/%v", partNum, fs.SizeSuffix(n), fs.SizeSuffix(partOff), fs.SizeSuffix(size))
			_, err = w.WriteChunk(gCtx, int(partNum), rw)
			return err
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	err = w.Close(ctx)
	if err != nil {
		return fmt.Errorf("multipart upload: failed to finalise: %w", err)
	}

	return nil
}

var warnStreamUpload sync.Once

// state of ChunkWriter
type ossChunkWriter struct {
	chunkSize     int64
	size          int64
	f             *Fs
	o             *Object
	in            io.Reader
	mu            sync.Mutex
	uploadedParts []oss.UploadPart
	bucket        *oss.Bucket
	callback      []oss.Option
	imur          oss.InitiateMultipartUploadResult
	// token
	token       *api.OSSToken
	done        chan any
	shutdown    sync.Once
	expiryTimer *time.Timer // signals whenever the token expires
}

func (f *Fs) newChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, ui *api.UploadInitInfo, in io.Reader, options ...fs.OpenOption) (w *ossChunkWriter, err error) {
	// Temporary Object under construction
	o := &Object{
		fs:     f,
		remote: remote,
	}

	uploadParts := f.opt.MaxUploadParts
	if uploadParts < 1 {
		uploadParts = 1
	} else if uploadParts > maxUploadParts {
		uploadParts = maxUploadParts
	}
	size := src.Size()

	// calculate size of parts
	chunkSize := f.opt.ChunkSize

	// size can be -1 here meaning we don't know the size of the incoming file. We use ChunkSize
	// buffers here (default 5 MiB). With a maximum number of parts (10,000) this will be a file of
	// 48 GiB which seems like a not too unreasonable limit.
	if size == -1 {
		warnStreamUpload.Do(func() {
			fs.Logf(f, "Streaming uploads using chunk size %v will have maximum file size of %v",
				f.opt.ChunkSize, fs.SizeSuffix(int64(chunkSize)*int64(uploadParts)))
		})
	} else {
		chunkSize = chunksize.Calculator(src, size, uploadParts, chunkSize)
	}

	w = &ossChunkWriter{
		chunkSize: int64(chunkSize),
		size:      size,
		f:         f,
		o:         o,
		in:        in,
	}

	w.bucket, w.callback, w.token, err = f.newOSSBucket(ctx, ui)
	if err != nil {
		return nil, fmt.Errorf("failed to create a new OSS bucket: %w", err)
	}

	err = w.f.pacer.Call(func() (bool, error) {
		w.imur, err = w.bucket.InitiateMultipartUpload(ui.Object, ossOpts(nil, options...)...)
		return w.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload failed: %w", err)
	}
	w.startRenew()
	fs.Debugf(w.o, "multipart upload: %q initiated", w.imur.UploadID)
	return
}

// shouldRetry returns a boolean as to whether this err
// deserve to be retried. It returns the err as a convenience
func (w *ossChunkWriter) shouldRetry(ctx context.Context, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if fserrors.ShouldRetry(err) {
		return true, err
	}

	switch ossErr := err.(type) {
	case *oss.ServiceError:
		if ossErr.StatusCode == 403 && (ossErr.Code == "InvalidAccessKeyId" || ossErr.Code == "SecurityTokenExpired") {
			// oss: service returned error: StatusCode=403, ErrorCode=InvalidAccessKeyId,
			// ErrorMessage="The OSS Access Key Id you provided does not exist in our records.",

			// oss: service returned error: StatusCode=403, ErrorCode=SecurityTokenExpired,
			// ErrorMessage="The security token you provided has expired."

			// These errors cannot be handled once token is expired. Should update token proactively.
			return false, fserrors.FatalError(err)
		}
	}
	return false, err
}

// add a part number and etag to the completed parts
func (w *ossChunkWriter) addCompletedPart(part oss.UploadPart) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.uploadedParts = append(w.uploadedParts, part)
}

// WriteChunk will write chunk number with reader bytes, where chunk number >= 0
func (w *ossChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (currentChunkSize int64, err error) {
	if chunkNumber < 0 {
		err := fmt.Errorf("invalid chunk number provided: %v", chunkNumber)
		return -1, err
	}

	ossPartNumber := chunkNumber + 1
	var uout oss.UploadPart
	err = w.f.pacer.Call(func() (bool, error) {
		// Discover the size by seeking to the end
		currentChunkSize, err = reader.Seek(0, io.SeekEnd)
		if err != nil {
			return false, err
		}
		// rewind the reader on retry and after reading md5
		_, err := reader.Seek(0, io.SeekStart)
		if err != nil {
			return false, err
		}
		uout, err = w.bucket.UploadPart(w.imur, reader, currentChunkSize, ossPartNumber)
		if err != nil {
			if chunkNumber <= 8 {
				return w.shouldRetry(ctx, err)
			}
			// retry all chunks once have done the first few
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return -1, fmt.Errorf("failed to upload chunk %d with %v bytes: %w", ossPartNumber, currentChunkSize, err)
	}

	w.addCompletedPart(uout)

	fs.Debugf(w.o, "multipart upload: wrote chunk %d with %v bytes", ossPartNumber, currentChunkSize)
	return currentChunkSize, err
}

// Abort the multipart upload
func (w *ossChunkWriter) Abort(ctx context.Context) (err error) {
	// Abort the upload session
	err = w.f.pacer.Call(func() (bool, error) {
		err = w.bucket.AbortMultipartUpload(w.imur)
		return w.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload %q: %w", w.imur.UploadID, err)
	}
	w.shutdownRenew()
	fs.Debugf(w.o, "multipart upload: %q aborted", w.imur.UploadID)
	return
}

// Close and finalise the multipart upload
func (w *ossChunkWriter) Close(ctx context.Context) (err error) {
	// Finalise the upload session
	err = w.f.pacer.Call(func() (bool, error) {
		_, err := w.bucket.CompleteMultipartUpload(w.imur, w.uploadedParts, w.callback...)
		return w.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	w.shutdownRenew()
	fs.Debugf(w.o, "multipart upload: %q finished", w.imur.UploadID)
	return
}

// ------------------------------------------------------------

// OnExpiry returns a channel which has the time written to it when
// the token expires.  Note that there is only one channel so if
// attaching multiple go routines it will only signal to one of them.
func (w *ossChunkWriter) OnExpiry() <-chan time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.expiryTimer == nil {
		w.expiryTimer = time.NewTimer(w.token.TimeToExpiry())
	}
	return w.expiryTimer.C
}

func (w *ossChunkWriter) renewToken(ctx context.Context) error {
	token, err := w.f.getOSSToken(ctx)
	if err != nil {
		fs.Errorf(w.o, "failed to update OSS token: %v", err)
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bucket.Client.Config.SecurityToken = token.SecurityToken
	w.bucket.Client.Config.AccessKeyID = token.AccessKeyID
	w.bucket.Client.Config.AccessKeySecret = token.AccessKeySecret
	w.token = token
	// Bump on the expiry timer if it is set
	if w.expiryTimer != nil {
		w.expiryTimer.Reset(w.token.TimeToExpiry())
	}
	return nil
}

func (w *ossChunkWriter) renewOnExpiry() {
	expiry := w.OnExpiry()
	for {
		select {
		case <-expiry:
		case <-w.done:
			return
		}
		fs.Debugf(w.o, "multipart upload: refreshing token...")
		// Do a transaction
		err := w.renewToken(context.Background())
		if err == nil {
			fs.Debugf(w.o, "multipart upload: token refresh successful")
		} else {
			fs.Errorf(w.o, "multipart upload: token refresh failed: %v", err)
		}
	}
}

// startRenew starts the token renewer
func (w *ossChunkWriter) startRenew() {
	w.done = make(chan any)
	go w.renewOnExpiry()
}

// shutdownRenew stops the timer and no more renewal will take place.
func (w *ossChunkWriter) shutdownRenew() {
	if w == nil {
		return
	}
	// closing a channel can only be done once
	w.shutdown.Do(func() {
		w.expiryTimer.Stop()
		close(w.done)
	})
}
