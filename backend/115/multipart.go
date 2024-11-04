// Implements multipart uploading for 115. Mostly from lib/multipart
package _115

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
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
	tokens := pacer.NewTokenDispenser(w.con)

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
	con           int
	f             *Fs
	o             *Object
	in            io.Reader
	mu            sync.Mutex
	uploadedParts []oss.UploadPart
	client        *oss.Client
	callback      string
	callbackVar   string
	callbackRes   map[string]any
	imur          *oss.InitiateMultipartUploadResult
}

func (f *Fs) newChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, ui *api.UploadInitInfo, in io.Reader, options ...fs.OpenOption) (w *ossChunkWriter, err error) {
	// Temporary Object under construction
	o := &Object{
		fs:     f,
		remote: remote,
	}

	uploadParts := min(max(1, f.opt.MaxUploadParts), maxUploadParts)
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
		con:       max(1, f.opt.UploadConcurrency),
		f:         f,
		o:         o,
		in:        in,
		client:    f.newOSSClient(),
	}

	req := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(ui.Bucket),
		Key:    oss.Ptr(ui.Object),
	}
	req.Parameters = map[string]string{"x-oss-enable-sha1": ""}
	if w.con == 1 {
		req.Parameters["sequential"] = ""
	}
	// Apply upload options
	for _, option := range options {
		key, value := option.Header()
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "":
			// ignore
		case "cache-control":
			req.CacheControl = oss.Ptr(value)
		case "content-disposition":
			req.ContentDisposition = oss.Ptr(value)
		case "content-encoding":
			req.ContentEncoding = oss.Ptr(value)
		case "content-type":
			req.ContentType = oss.Ptr(value)
		}
	}
	err = w.f.pacer.Call(func() (bool, error) {
		w.imur, err = w.client.InitiateMultipartUpload(ctx, req)
		return w.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload failed: %w", err)
	}
	w.callback, w.callbackVar = ui.GetCallback(), ui.GetCallbackVar()
	fs.Debugf(w.o, "multipart upload: %q initiated", *w.imur.UploadId)
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
	var res *oss.UploadPartResult
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
		res, err = w.client.UploadPart(ctx, &oss.UploadPartRequest{
			Bucket:     w.imur.Bucket,
			Key:        w.imur.Key,
			UploadId:   w.imur.UploadId,
			PartNumber: int32(ossPartNumber),
			Body:       reader,
		})
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

	w.addCompletedPart(oss.UploadPart{
		PartNumber: int32(ossPartNumber),
		ETag:       res.ETag,
	})

	fs.Debugf(w.o, "multipart upload: wrote chunk %d with %v bytes", ossPartNumber, currentChunkSize)
	return currentChunkSize, err
}

// Abort the multipart upload
func (w *ossChunkWriter) Abort(ctx context.Context) (err error) {
	// Abort the upload session
	err = w.f.pacer.Call(func() (bool, error) {
		_, err = w.client.AbortMultipartUpload(ctx, &oss.AbortMultipartUploadRequest{
			Bucket:   w.imur.Bucket,
			Key:      w.imur.Key,
			UploadId: w.imur.UploadId,
		})
		return w.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("failed to abort multipart upload %q: %w", *w.imur.UploadId, err)
	}
	// w.shutdownRenew()
	fs.Debugf(w.o, "multipart upload: %q aborted", *w.imur.UploadId)
	return
}

// Close and finalise the multipart upload
func (w *ossChunkWriter) Close(ctx context.Context) (err error) {
	// Finalise the upload session
	var res *oss.CompleteMultipartUploadResult
	err = w.f.pacer.Call(func() (bool, error) {
		res, err = w.client.CompleteMultipartUpload(ctx, &oss.CompleteMultipartUploadRequest{
			Bucket:   w.imur.Bucket,
			Key:      w.imur.Key,
			UploadId: w.imur.UploadId,
			CompleteMultipartUpload: &oss.CompleteMultipartUpload{
				Parts: w.uploadedParts,
			},
			Callback:    oss.Ptr(w.callback),
			CallbackVar: oss.Ptr(w.callbackVar),
		})
		return w.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	w.callbackRes = res.CallbackResult
	fs.Debugf(w.o, "multipart upload: %q finished", *w.imur.UploadId)
	return
}
