package _115

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/backend/115/cipher"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/rest"
)

// Globals
const (
	cachePrefix  = "rclone-115-sha1sum-"
	md5Salt      = "Qclm8MGWUv59TnrR0XPg"
	OSSEndpoint  = "http://oss-cn-shenzhen.aliyuncs.com" // https://uplb.115.com/3.0/getuploadinfo.php
	OSSUserAgent = "aliyun-sdk-android/2.9.1"
)

func (f *Fs) getUploadBasicInfo(ctx context.Context) (err error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://proapi.115.com/app/uploadinfo",
	}
	var info *api.UploadBasicInfo
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err != nil {
		return
	} else if !info.State {
		return fmt.Errorf("API State false: %s (%d)", info.Error, info.Errno)
	}
	userID := info.UserID.String()
	if userID == "0" {
		return errors.New("invalid user id")
	}
	f.userID = userID
	f.userkey = info.Userkey
	return
}

func bufferIO(in io.Reader, size, threshold int64) (out io.Reader, cleanup func(), err error) {
	// nothing to clean up by default
	cleanup = func() {}

	// don't cache small files on disk to reduce wear of the disk
	if size > threshold {
		var tempFile *os.File

		// create the cache file
		tempFile, err = os.CreateTemp("", cachePrefix)
		if err != nil {
			return
		}

		_ = os.Remove(tempFile.Name()) // Delete the file - may not work on Windows

		// clean up the file after we are done downloading
		cleanup = func() {
			// the file should normally already be close, but just to make sure
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name()) // delete the cache file after we are done - may be deleted already
		}

		// copy the ENTIRE file to disc and calculate the SHA1 in the process
		if _, err = io.Copy(tempFile, in); err != nil {
			return
		}
		// jump to the start of the local file so we can pass it along
		if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
			return
		}

		// replace the already read source with a reader of our cached file
		out = tempFile
	} else {
		// that's a small file, just read it into memory
		var inData []byte
		inData, err = io.ReadAll(in)
		if err != nil {
			return
		}

		// set the reader to our read memory block
		out = bytes.NewReader(inData)
	}
	return out, cleanup, nil
}

func bufferIOwithSHA1(in io.Reader, size, threshold int64) (sha1sum string, out io.Reader, cleanup func(), err error) { // we need an SHA1
	hash := sha1.New()
	// use the tee to write to the local file AND calculate the SHA1 while doing so
	tee := io.TeeReader(in, hash)
	out, cleanup, err = bufferIO(tee, size, threshold)
	sha1sum = hex.EncodeToString(hash.Sum(nil))
	return
}

func generateSignature(userID, fileID, target, userKey string) string {
	sha1sum := sha1.Sum([]byte(userID + fileID + target + "0"))
	sigStr := userKey + hex.EncodeToString(sha1sum[:]) + "000000"
	sh1Sig := sha1.Sum([]byte(sigStr))
	return strings.ToUpper(hex.EncodeToString(sh1Sig[:]))
}

func generateToken(userID, fileID, fileSize, signKey, signVal, timeStamp string) string {
	userIDMd5 := md5.Sum([]byte(userID))
	tokenMd5 := md5.Sum([]byte(md5Salt + fileID + fileSize + signKey + signVal + userID + timeStamp + hex.EncodeToString(userIDMd5[:]) + appVer))
	return hex.EncodeToString(tokenMd5[:])
}

func (f *Fs) initUpload(ctx context.Context, size int64, name, dirID, sha1sum, signKey, signVal string) (info *api.UploadInitInfo, err error) {
	var (
		userID       = f.userID
		userKey      = f.userkey
		filename     = f.opt.Enc.FromStandardName(name)
		filesize     = strconv.FormatInt(size, 10)
		fileID       = strings.ToUpper(sha1sum)
		target       = "U_1_" + dirID            // target id
		ts           = int64(time.Now().Unix())  // timestamp in int64
		t            = strconv.FormatInt(ts, 10) // timestamp in string
		ecdhCipher   *cipher.EcdhCipher
		encodedToken string
		encrypted    []byte
		decrypted    []byte
	)

	if ecdhCipher, err = cipher.NewEcdhCipher(); err != nil {
		return
	}

	// url parameter
	if encodedToken, err = ecdhCipher.EncodeToken(ts); err != nil {
		return
	}

	// form that will be encrypted
	form := url.Values{}
	form.Set("appid", "0")
	form.Set("appversion", appVer) // const
	form.Set("userid", userID)
	form.Set("filename", filename)
	form.Set("filesize", filesize)
	form.Set("fileid", fileID)
	form.Set("target", target)
	form.Set("sig", generateSignature(userID, fileID, target, userKey))
	form.Set("t", t)
	form.Set("token", generateToken(userID, fileID, filesize, signKey, signVal, t))
	if signKey != "" && signVal != "" {
		form.Set("sign_key", signKey)
		form.Set("sign_val", signVal)
	}
	if encrypted, err = ecdhCipher.Encrypt([]byte(form.Encode())); err != nil {
		return
	}

	opts := rest.Opts{
		Method:      "POST",
		RootURL:     "https://uplb.115.com/4.0/initupload.php",
		ContentType: "application/x-www-form-urlencoded",
		Parameters:  url.Values{"k_ec": {encodedToken}},
		Body:        bytes.NewReader(encrypted),
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, nil, err)
	})
	if err != nil {
		return
	}
	body, err := rest.ReadBody(resp)
	if err != nil {
		return
	}
	if decrypted, err = ecdhCipher.Decrypt(body); err != nil {
		// FIXME failed to decrypt intermittenly
		// seems to be caused by corrupted body
		// low level retry deosn't help
		return
	}
	if err = json.Unmarshal(decrypted, &info); err != nil {
		return
	}
	switch info.ErrorCode {
	case 0:
		return
	case 701: // when status == 7
		return
	default:
		return nil, fmt.Errorf("%s (%d)", info.ErrorMsg, info.ErrorCode)
	}
}

func (f *Fs) getOSSToken(ctx context.Context) (info *api.OSSToken, err error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: "https://uplb.115.com/3.0/gettoken.php", // https://uplb.115.com/3.0/getuploadinfo.php
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err == nil && info.StatusCode != "200" {
		return nil, fmt.Errorf("StateCode: %s", info.StatusCode)
	}
	return
}

func ossOpts(ossOptions []oss.Option, options ...fs.OpenOption) []oss.Option {
	opts := ossOptions

	// Apply upload options
	for _, option := range options {
		key, value := option.Header()
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "":
			// ignore
		case "cache-control":
			opts = append(opts, oss.CacheControl(value))
		case "content-disposition":
			opts = append(opts, oss.ContentDisposition(value))
		case "content-encoding":
			opts = append(opts, oss.ContentEncoding(value))
		}
	}
	return opts
}

func (f *Fs) newOSSBucket(ctx context.Context, ui *api.UploadInitInfo) (bucket *oss.Bucket, callback []oss.Option, expire []oss.Option, err error) {
	token, err := f.getOSSToken(ctx)
	if err != nil {
		err = fmt.Errorf("failed to get OSS token: %w", err)
		return
	}
	cliOpts := []oss.ClientOption{
		oss.HTTPClient(f.client),
		oss.SecurityToken(token.SecurityToken),
		oss.UserAgent(OSSUserAgent),
	}
	client, err := oss.New(OSSEndpoint, token.AccessKeyID, token.AccessKeySecret, cliOpts...)
	if err != nil {
		err = fmt.Errorf("failed to create a new OSS client: %w", err)
		return
	}
	bucket, err = client.Bucket(ui.Bucket)
	if err != nil {
		err = fmt.Errorf("failed to get OSS bucket instance: %w", err)
		return
	}
	callback = []oss.Option{
		oss.Callback(base64.StdEncoding.EncodeToString([]byte(ui.Callback.Callback))),
		oss.CallbackVar(base64.StdEncoding.EncodeToString([]byte(ui.Callback.CallbackVar))),
	}
	expire = []oss.Option{oss.Expires(token.Expiration)}
	return
}

func calcBlockSHA1(ctx context.Context, in io.Reader, src fs.ObjectInfo, rangeSpec string) (sha1sum string, err error) {
	var start, end int64
	if _, err = fmt.Sscanf(rangeSpec, "%d-%d", &start, &end); err != nil {
		return
	}

	hash := sha1.New()
	if ra, ok := in.(io.ReaderAt); ok {
		reader := io.NewSectionReader(ra, start, end-start+1)
		if _, err = io.Copy(hash, reader); err == nil {
			sha1sum = strings.ToUpper(hex.EncodeToString(hash.Sum(nil)))
		}
	} else {
		srcObj := fs.UnWrapObjectInfo(src)
		rc, err := srcObj.Open(ctx, &fs.RangeOption{Start: start, End: end})
		if err != nil {
			return "", fmt.Errorf("failed to open source: %w", err)
		}
		defer fs.CheckClose(rc, &err)
		if _, err = io.Copy(hash, rc); err == nil {
			sha1sum = strings.ToUpper(hex.EncodeToString(hash.Sum(nil)))
		}
	}
	return
}

// upload uploads the object with or without using a temporary file name
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote string, options ...fs.OpenOption) (fs.Object, error) {
	size := src.Size()

	// check upload available
	if f.userID == "" {
		if err := f.getUploadBasicInfo(ctx); err != nil {
			return nil, fmt.Errorf("failed to get upload basic info: %w", err)
		}
	}
	if size > maxUploadSize {
		return nil, fmt.Errorf("file size exceeds the upload limit: %d > %d", size, int64(maxUploadSize))
	}

	// Create a new object with its parent directory if it doesn't exist
	o, leaf, dirID, err := f.createObject(ctx, remote, src.ModTime(ctx), size)
	if err != nil {
		return nil, err
	}

	var wrap accounting.WrapFn
	var cleanup func()

	// Calculate sha1sum; grabbed from package jottacloud
	hashStr, err := src.Hash(ctx, hash.SHA1)
	if err != nil || hashStr == "" {
		fs.Debugf(o, "Buffering to calculate SHA1...")
		// unwrap the accounting from the input, we use wrap to put it
		// back on after the buffering
		in, wrap = accounting.UnWrap(in)
		hashStr, in, cleanup, err = bufferIOwithSHA1(in, size, int64(f.opt.HashMemoryThreshold))
		defer cleanup()
		if err != nil {
			return nil, fmt.Errorf("failed to calculate SHA1: %w", err)
		}
	} else {
		fs.Debugf(o, "Using SHA1 from src: %s", hashStr)
	}

	// set calculated sha1 hash
	o.sha1sum = strings.ToLower(hashStr)

	var ui *api.UploadInitInfo
	signKey, signVal := "", ""
	for retry := true; retry; {
		ui, err = f.initUpload(ctx, size, leaf, dirID, hashStr, signKey, signVal)
		if err != nil {
			return nil, fmt.Errorf("failed to init upload: %w", err)
		}
		retry = ui.Status == 7
		switch ui.Status {
		case 1:
			fs.Debugf(o, "Upload will begin shortly. Outgoing traffic will occur")
		case 2:
			fs.Debugf(o, "Upload finished early. No outgoing traffic will occur")
			return o, nil
		case 7:
			signKey = ui.SignKey
			if signVal, err = calcBlockSHA1(ctx, in, src, ui.SignCheck); err != nil {
				return nil, fmt.Errorf("failed to calculate block hash: %w", err)
			}
			fs.Debugf(o, "Retrying init upload: Status 7")
		default:
			return nil, fmt.Errorf("unexpected status: %#v", ui)
		}
	}

	// Wrap the accounting back onto the stream
	if wrap != nil {
		in = wrap(in)
	}

	if size < 0 || size >= int64(o.fs.opt.UploadCutoff) {
		mu, err := f.newChunkWriter(ctx, remote, src, ui, in, options...)
		if err != nil {
			return nil, fmt.Errorf("multipart upload failed to initialise: %w", err)
		}
		return o, mu.Upload(ctx)
	}

	// upload singlepart
	bucket, callback, _, err := f.newOSSBucket(ctx, ui)
	if err != nil {
		return nil, fmt.Errorf("failed to get OSS bucket: %w", err)
	}
	return o, bucket.PutObject(ui.Object, in, ossOpts(callback, options...)...)
}
