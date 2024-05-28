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
	"github.com/orzogc/fake115uploader/cipher"
	"github.com/rclone/rclone/backend/115/api"
	"github.com/rclone/rclone/lib/rest"
)

// Globals
const (
	appVer      = "2.0.3.6"
	cachePrefix = "rclone-115-sha1sum-"
	md5Salt     = "Qclm8MGWUv59TnrR0XPg"
	OSSEndpoint = "cn-shenzhen.oss.aliyuncs.com"
	OSSRegionID = "oss-cn-shenzhen"

	OSSUserAgent               = "aliyun-sdk-android/2.9.1"
	OssSecurityTokenHeaderName = "X-OSS-Security-Token"
)

func (f *Fs) getUploadInfo(ctx context.Context) (info *api.UploadInfo, err error) {
	opts := rest.Opts{
		Method:  "POST",
		RootURL: "https://proapi.115.com/app/uploadinfo",
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, info, err)
	})
	if err == nil && !info.State {
		return nil, fmt.Errorf("API State false: %s (%d)", info.Error, info.Errno)
	}
	return
}

func bufferIO(in io.Reader, size, threshold int64) (out io.Reader, cleanup func(), err error) {
	// nothing to clean up by default
	cleanup = func() {}

	// don't cache small files on disk to reduce wear of the disk
	// if true {
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
		userID       = f.ui.UserID.String()
		userKey      = f.ui.Userkey
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
		resp, err = f.srv.CallJSON(ctx, &opts, nil, nil)
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
		return
	}
	if err = json.Unmarshal(decrypted, &info); err != nil {
		return
	}
	switch info.ErrorCode {
	case 0:
		return
	case 701:
		return
	default:
		return nil, fmt.Errorf("%s (%d)", info.ErrorMsg, info.ErrorCode)
	}
}

func (f *Fs) getOSSToken(ctx context.Context) (info *api.OSSToken, err error) {
	opts := rest.Opts{
		Method:  "POST",
		RootURL: "https://uplb.115.com/3.0/gettoken.php",
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

func (f *Fs) uploadSinglepart(ctx context.Context, params *api.UploadInitInfo, r io.Reader) error {
	ossToken, err := f.getOSSToken(ctx)
	if err != nil {
		return err
	}
	ossClient, err := oss.New(OSSEndpoint, ossToken.AccessKeyID, ossToken.AccessKeySecret)
	if err != nil {
		return err
	}
	bucket, err := ossClient.Bucket(params.Bucket)
	if err != nil {
		return err
	}
	options := []oss.Option{
		oss.SetHeader(OssSecurityTokenHeaderName, ossToken.SecurityToken),
		oss.Callback(base64.StdEncoding.EncodeToString([]byte(params.Callback.Callback))),
		oss.CallbackVar(base64.StdEncoding.EncodeToString([]byte(params.Callback.CallbackVar))),
		oss.UserAgentHeader(OSSUserAgent),
	}
	if err = bucket.PutObject(params.Object, r, options...); err != nil {
		return err
	}
	return nil
}

func calcBlockSHA1(in io.Reader, rangeSpec string) (sha1sum string, err error) {
	var start, end int64
	if _, err = fmt.Sscanf(rangeSpec, "%d-%d", &start, &end); err != nil {
		return
	}
	length := end - start + 1

	var reader io.Reader
	if r, ok := in.(io.ReaderAt); ok {
		reader = io.NewSectionReader(r, start, length)
	} else {
		return "", errors.New("input stream doesn't support io.ReaderAt")
	}

	hash := sha1.New()
	if _, err = io.Copy(hash, reader); err == nil {
		sha1sum = strings.ToUpper(hex.EncodeToString(hash.Sum(nil)))
	}
	return
}

// // SplitFileByPartNum splits big file into parts by the num of parts.
// // Split the file with specified parts count, returns the split result when error is nil.
// func SplitFileByPartNum(fileSize int64, chunkNum int) ([]oss.FileChunk, error) {
// 	if chunkNum <= 0 || chunkNum > 10000 {
// 		return nil, errors.New("chunkNum invalid")
// 	}

// 	if int64(chunkNum) > fileSize {
// 		return nil, errors.New("oss: chunkNum invalid")
// 	}

// 	var chunks []oss.FileChunk
// 	var chunk = oss.FileChunk{}
// 	var chunkN = (int64)(chunkNum)
// 	for i := int64(0); i < chunkN; i++ {
// 		chunk.Number = int(i + 1)
// 		chunk.Offset = i * (fileSize / chunkN)
// 		if i == chunkN-1 {
// 			chunk.Size = fileSize/chunkN + fileSize%chunkN
// 		} else {
// 			chunk.Size = fileSize / chunkN
// 		}
// 		chunks = append(chunks, chunk)
// 	}

// 	return chunks, nil
// }

// // SplitFileByPartSize splits big file into parts by the size of parts.
// // Splits the file by the part size. Returns the FileChunk when error is nil.
// func SplitFileByPartSize(fileSize int64, chunkSize int64) ([]oss.FileChunk, error) {
// 	if chunkSize <= 0 {
// 		return nil, errors.New("chunkSize invalid")
// 	}

// 	var chunkN = fileSize / chunkSize
// 	if chunkN >= 10000 {
// 		return nil, errors.New("too many parts, please increase part size")
// 	}

// 	var chunks []oss.FileChunk
// 	var chunk = oss.FileChunk{}
// 	for i := int64(0); i < chunkN; i++ {
// 		chunk.Number = int(i + 1)
// 		chunk.Offset = i * chunkSize
// 		chunk.Size = chunkSize
// 		chunks = append(chunks, chunk)
// 	}

// 	if fileSize%chunkSize > 0 {
// 		chunk.Number = len(chunks) + 1
// 		chunk.Offset = int64(len(chunks)) * chunkSize
// 		chunk.Size = fileSize % chunkSize
// 		chunks = append(chunks, chunk)
// 	}

// 	return chunks, nil
// }

// func SplitFile(fileSize int64) (chunks []oss.FileChunk, err error) {
// 	for i := int64(1); i < 10; i++ {
// 		if fileSize < i*int64(fs.Gibi) { // 文件大小小于iGB时分为i*1000片
// 			if chunks, err = SplitFileByPartNum(fileSize, int(i*1000)); err != nil {
// 				return
// 			}
// 			break
// 		}
// 	}
// 	if fileSize > 9*int64(fs.Gibi) { // 文件大小大于9GB时分为10000片
// 		if chunks, err = SplitFileByPartNum(fileSize, 10000); err != nil {
// 			return
// 		}
// 	}
// 	// 单个分片大小不能小于100KB
// 	if chunks[0].Size < 100*int64(fs.Kibi) {
// 		if chunks, err = SplitFileByPartSize(fileSize, 100*int64(fs.Kibi)); err != nil {
// 			return
// 		}
// 	}
// 	return
// }

// func chunksProducer(ch chan oss.FileChunk, chunks []oss.FileChunk) {
// 	for _, chunk := range chunks {
// 		ch <- chunk
// 	}
// }

// // OssOption get options
// func OssOption(params *api.UploadInitInfo, ossToken *api.OSSToken) []oss.Option {
// 	options := []oss.Option{
// 		oss.SetHeader(OssSecurityTokenHeaderName, ossToken.SecurityToken),
// 		oss.Callback(base64.StdEncoding.EncodeToString([]byte(params.Callback.Callback))),
// 		oss.CallbackVar(base64.StdEncoding.EncodeToString([]byte(params.Callback.CallbackVar))),
// 		oss.UserAgentHeader(OSSUserAgent),
// 	}
// 	return options
// }

// // UploadByMultipart upload by mutipart blocks
// func (f *Fs) uploadMultipart(ctx context.Context, params *api.UploadInitInfo, fileSize int64, in io.Reader, opts ...driver115.UploadMultipartOption) error {
// 	var (
// 		chunks    []oss.FileChunk
// 		parts     []oss.UploadPart
// 		imur      oss.InitiateMultipartUploadResult
// 		ossClient *oss.Client
// 		bucket    *oss.Bucket
// 		ossToken  *api.OSSToken
// 		err       error
// 	)

// 	// in, _ = accounting.UnWrapAccounting(in)
// 	tmpF, ok := in.(io.ReaderAt)
// 	if !ok {
// 		return errors.New("not support ReaderAt")
// 	}

// 	options := driver115.DefalutUploadMultipartOptions()
// 	if len(opts) > 0 {
// 		for _, f := range opts {
// 			f(options)
// 		}
// 	}

// 	if ossToken, err = f.getOSSToken(ctx); err != nil {
// 		return err
// 	}

// 	if ossClient, err = oss.New(driver115.OSSEndpoint, ossToken.AccessKeyID, ossToken.AccessKeySecret); err != nil {
// 		return err
// 	}

// 	if bucket, err = ossClient.Bucket(params.Bucket); err != nil {
// 		return err
// 	}

// 	// ossToken一小时后就会失效，所以每50分钟重新获取一次
// 	ticker := time.NewTicker(options.TokenRefreshTime)
// 	defer ticker.Stop()
// 	// 设置超时
// 	timeout := time.NewTimer(options.Timeout)

// 	if chunks, err = SplitFile(fileSize); err != nil {
// 		return err
// 	}

// 	if imur, err = bucket.InitiateMultipartUpload(params.Object,
// 		oss.SetHeader(driver115.OssSecurityTokenHeaderName, ossToken.SecurityToken),
// 		oss.UserAgentHeader(driver115.OSSUserAgent),
// 	); err != nil {
// 		return err
// 	}

// 	wg := sync.WaitGroup{}
// 	wg.Add(len(chunks))

// 	chunksCh := make(chan oss.FileChunk)
// 	errCh := make(chan error)
// 	UploadedPartsCh := make(chan oss.UploadPart)
// 	quit := make(chan struct{})

// 	// producer
// 	go chunksProducer(chunksCh, chunks)
// 	go func() {
// 		wg.Wait()
// 		quit <- struct{}{}
// 	}()

// 	// consumers
// 	for i := 0; i < options.ThreadsNum; i++ {
// 		go func(threadId int) {
// 			defer func() {
// 				if r := recover(); r != nil {
// 					errCh <- fmt.Errorf("recovered in %v", r)
// 				}
// 			}()
// 			for chunk := range chunksCh {
// 				var part oss.UploadPart // 出现错误就继续尝试，共尝试3次
// 				for retry := 0; retry < 3; retry++ {
// 					select {
// 					case <-ticker.C:
// 						if ossToken, err = f.getOSSToken(ctx); err != nil { // 到时重新获取ossToken
// 							errCh <- fmt.Errorf("刷新token时出现错误: %w", err)
// 						}
// 					default:
// 					}

// 					buf := make([]byte, chunk.Size)
// 					if _, err = tmpF.ReadAt(buf, chunk.Offset); err != nil && !errors.Is(err, io.EOF) {
// 						continue
// 					}

// 					b := bytes.NewBuffer(buf)
// 					if part, err = bucket.UploadPart(imur, b, chunk.Size, chunk.Number, OssOption(params, ossToken)...); err == nil {
// 						break
// 					}
// 				}
// 				if err != nil {
// 					errCh <- fmt.Errorf("上传 %s 的第%d个分片时出现错误：%w", "filename", chunk.Number, err)
// 				}
// 				UploadedPartsCh <- part
// 			}
// 		}(i)
// 	}

// 	go func() {
// 		for part := range UploadedPartsCh {
// 			parts = append(parts, part)
// 			wg.Done()
// 		}
// 	}()
// LOOP:
// 	for {
// 		select {
// 		case <-ticker.C:
// 			// 到时重新获取ossToken
// 			if ossToken, err = f.getOSSToken(ctx); err != nil {
// 				return err
// 			}
// 		case <-quit:
// 			break LOOP
// 		case <-errCh:
// 			return err
// 		case <-timeout.C:
// 			return fmt.Errorf("time out")
// 		}
// 	}

// 	// EOF错误是xml的Unmarshal导致的，响应其实是json格式，所以实际上上传是成功的
// 	if _, err = bucket.CompleteMultipartUpload(imur, parts, OssOption(params, ossToken)...); err != nil && !errors.Is(err, io.EOF) {
// 		// 当文件名含有 &< 这两个字符之一时响应的xml解析会出现错误，实际上上传是成功的
// 		if filename := filepath.Base("filename"); !strings.ContainsAny(filename, "&<") {
// 			return err
// 		}
// 	}
// 	return nil
// }
