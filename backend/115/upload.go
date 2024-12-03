package _115

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
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

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/backend/115/cipher"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/rest"
)

// Globals
const (
	cachePrefix  = "rclone-115-sha1sum-"
	md5Salt      = "Qclm8MGWUv59TnrR0XPg"
	OSSRegion    = "cn-shenzhen" // https://uplb.115.com/3.0/getuploadinfo.php
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
		return fmt.Errorf("API Error: %s (%d)", info.Error, info.Errno)
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

// ------------------------------------------------------------

func generateSignature(userID, fileID, target, userKey string) string {
	sha1sum := sha1.Sum([]byte(userID + fileID + target + "0"))
	sigStr := userKey + hex.EncodeToString(sha1sum[:]) + "000000"
	sh1Sig := sha1.Sum([]byte(sigStr))
	return strings.ToUpper(hex.EncodeToString(sh1Sig[:]))
}

func generateToken(userID, fileID, fileSize, signKey, signVal, timeStamp, appVer string) string {
	userIDMd5 := md5.Sum([]byte(userID))
	tokenMd5 := md5.Sum([]byte(md5Salt + fileID + fileSize + signKey + signVal + userID + timeStamp + hex.EncodeToString(userIDMd5[:]) + appVer))
	return hex.EncodeToString(tokenMd5[:])
}

func (f *Fs) initUpload(ctx context.Context, size int64, name, dirID, sha1sum, signKey, signVal string) (info *api.UploadInitInfo, err error) {
	var (
		filename     = f.opt.Enc.FromStandardName(name)
		filesize     = strconv.FormatInt(size, 10)
		fileID       = strings.ToUpper(sha1sum)
		target       = "U_1_" + dirID            // target id
		ts           = time.Now().UnixMilli()    // timestamp in int64
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
	form.Set("appversion", f.appVer)
	form.Set("userid", f.userID)
	form.Set("filename", filename)
	form.Set("filesize", filesize)
	form.Set("fileid", fileID)
	form.Set("target", target)
	form.Set("sig", generateSignature(f.userID, fileID, target, f.userkey))
	form.Set("t", t)
	form.Set("token", generateToken(f.userID, fileID, filesize, signKey, signVal, t, f.appVer))
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
		// low level retry doesn't help
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

func (f *Fs) postUpload(v map[string]any) (*api.CallbackData, error) {
	callbackJson, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var info api.CallbackInfo
	if err := json.Unmarshal(callbackJson, &info); err != nil {
		return nil, err
	}
	if !info.State {
		return nil, fmt.Errorf("API Error: %s (%d)", info.Message, info.Code)
	}
	return info.Data, nil
}

// ------------------------------------------------------------

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
		return nil, fmt.Errorf("failed to get OSS token: %s (%s)", info.ErrorMessage, info.ErrorCode)
	}
	return
}

func (f *Fs) newOSSClient() (client *oss.Client) {
	fetcher := credentials.CredentialsFetcherFunc(func(ctx context.Context) (credentials.Credentials, error) {
		t, err := f.getOSSToken(ctx)
		if err != nil {
			return credentials.Credentials{}, err
		}
		return credentials.Credentials{
			AccessKeyID:     t.AccessKeyID,
			AccessKeySecret: t.AccessKeySecret,
			SecurityToken:   t.SecurityToken,
			Expires:         &t.Expiration}, nil
	})
	provider := credentials.NewCredentialsFetcherProvider(fetcher)

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(provider).
		WithRegion(OSSRegion).
		WithUserAgent(OSSUserAgent)

	return oss.NewClient(cfg)
}

// unWrapObjectInfo returns the underlying Object unwrapped as much as
// possible or nil.
func unWrapObjectInfo(oi fs.ObjectInfo) fs.Object {
	if o, ok := oi.(fs.Object); ok {
		return fs.UnWrapObject(o)
	} else if do, ok := oi.(*fs.OverrideRemote); ok {
		// Unwrap if it is an operations.OverrideRemote
		return do.UnWrap()
	}
	return nil
}

func calcBlockSHA1(ctx context.Context, in io.Reader, src fs.ObjectInfo, rangeSpec string) (sha1sum string, err error) {
	var start, end int64
	if _, err = fmt.Sscanf(rangeSpec, "%d-%d", &start, &end); err != nil {
		return
	}

	var reader io.Reader
	if ra, ok := in.(io.ReaderAt); ok {
		reader = io.NewSectionReader(ra, start, end-start+1)
	} else if srcObj := unWrapObjectInfo(src); srcObj != nil {
		rc, err := srcObj.Open(ctx, &fs.RangeOption{Start: start, End: end})
		if err != nil {
			return "", fmt.Errorf("failed to open source: %w", err)
		}
		defer fs.CheckClose(rc, &err)
		reader = rc
	} else {
		return "", fmt.Errorf("failed to get reader from source %s", src)
	}

	hash := sha1.New()
	if _, err = io.Copy(hash, reader); err == nil {
		sha1sum = strings.ToUpper(hex.EncodeToString(hash.Sum(nil)))
	}
	return
}

// upload uploads the object with or without using a temporary file name
func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote string, options ...fs.OpenOption) (fs.Object, error) {
	if f.isShare {
		return nil, errors.New("unsupported for shared filesystem")
	}
	size := src.Size()

	// check upload available
	if f.userkey == "" {
		if err := f.getUploadBasicInfo(ctx); err != nil {
			return nil, fmt.Errorf("failed to get upload basic info: %w", err)
		}
		if f.userID == "" || f.userkey == "" {
			return nil, fmt.Errorf("empty userid or userkey")
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

	// Calculate sha1sum; grabbed from package jottacloud
	hashStr, err := src.Hash(ctx, hash.SHA1)
	if err != nil || hashStr == "" {
		if f.opt.UploadHashOnly {
			return nil, fserrors.NoRetryError(errors.New("skipping as no SHA1 from src"))
		}
		fs.Debugf(o, "Buffering to calculate SHA1...")
		var cleanup func()
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
			// In this case, the upload (perhaps via hash) could be successful,
			/// so let the subsequent process locate the uploaded object.
			return o, fmt.Errorf("failed to init upload: %w", err)
		}
		retry = ui.Status == 7
		switch ui.Status {
		case 1:
			if f.opt.UploadHashOnly {
				return nil, fserrors.NoRetryError(errors.New("skipping as --115-upload-hash-only flag turned on"))
			}
			fs.Debugf(o, "Upload will begin shortly. Outgoing traffic will occur...")
		case 2:
			// ui gives valid pickcode in this case but not available when listing
			fs.Debugf(o, "Upload finished early. No outgoing traffic will occur!")
			if acc, ok := in.(*accounting.Account); ok && acc != nil {
				// if `in io.Reader` is still in type of `*accounting.Account` (meaning that it is unused)
				// it is considered as a server side copy as no incoming/outgoing traffic occur at all
				acc.ServerSideTransferStart()
				acc.ServerSideCopyEnd(size)
			}
			if info, err := f.getFile(ctx, "", ui.PickCode); err == nil {
				return o, o.setMetaData(info)
			}
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

	if size < 0 || size >= int64(o.fs.opt.UploadCutoff) {
		mu, err := f.newChunkWriter(ctx, remote, src, ui, in, options...)
		if err != nil {
			return nil, fmt.Errorf("multipart upload failed to initialise: %w", err)
		}
		if err = mu.Upload(ctx); err != nil {
			return nil, err
		}
		data, err := f.postUpload(mu.callbackRes)
		if err != nil {
			return nil, fmt.Errorf("multipart upload failed to finalize: %w", err)
		}
		return o, o.setMetaDataFromCallBack(data)
	}

	// upload singlepart
	client := f.newOSSClient()
	req := &oss.PutObjectRequest{
		Bucket:      oss.Ptr(ui.Bucket),
		Key:         oss.Ptr(ui.Object),
		Body:        in,
		Callback:    oss.Ptr(ui.GetCallback()),
		CallbackVar: oss.Ptr(ui.GetCallbackVar()),
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

	res, err := client.PutObject(ctx, req)
	if err != nil {
		return nil, err
	}
	data, err := f.postUpload(res.CallbackResult)
	if err != nil {
		return nil, fmt.Errorf("failed to finalize upload: %w", err)
	}
	return o, o.setMetaDataFromCallBack(data)
}
