package _115 // nolint:revive

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteChunkRecoversPartAlreadyExist(t *testing.T) {
	tests := []struct {
		name     string
		partSize int64
		etag     string
		wantErr  bool
	}{
		{name: "matching part", partSize: 4, etag: fmt.Sprintf(`"%x"`, md5.Sum([]byte("data")))},
		{name: "mismatched part size", partSize: 3, etag: fmt.Sprintf(`"%x"`, md5.Sum([]byte("data"))), wantErr: true},
		{name: "mismatched part content", partSize: 4, etag: `"wrong-etag"`, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var uploadPartCalls, listPartsCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPut:
					uploadPartCalls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusConflict)
					_, _ = io.WriteString(w, `<Error><Code>PartAlreadyExist</Code><Message>For sequential multipart upload, you can't overwrite uploaded parts.</Message></Error>`)
				case http.MethodGet:
					listPartsCalls.Add(1)
					w.Header().Set("Content-Type", "application/xml")
					_, _ = io.WriteString(w, `<ListPartsResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-id</UploadId><PartNumberMarker>3</PartNumberMarker><MaxParts>1</MaxParts><IsTruncated>false</IsTruncated><Part><PartNumber>4</PartNumber><ETag>`+test.etag+`</ETag><Size>`+fmt.Sprint(test.partSize)+`</Size></Part></ListPartsResult>`)
				default:
					http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			ctx, ci := fs.AddConfig(context.Background())
			ci.LowLevelRetries = 3
			client := oss.NewClient(oss.LoadDefaultConfig().
				WithCredentialsProvider(credentials.NewAnonymousCredentialsProvider()).
				WithRegion(OSSRegion).
				WithEndpoint(server.URL))
			f := &Fs{pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(0), pacer.MaxSleep(0)))}
			w := &ossChunkWriter{
				con:    1,
				f:      f,
				client: client,
				imur: &oss.InitiateMultipartUploadResult{
					Bucket:   new("bucket"),
					Key:      new("key"),
					UploadId: new("upload-id"),
				},
			}

			n, err := w.WriteChunk(ctx, 3, bytes.NewReader([]byte("data")))
			if test.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "failed to recover existing chunk 4")
			} else {
				require.NoError(t, err)
				assert.Equal(t, int64(4), n)
				require.Len(t, w.uploadedParts, 1)
				assert.Equal(t, int32(4), w.uploadedParts[0].PartNumber)
				require.NotNil(t, w.uploadedParts[0].ETag)
				assert.Equal(t, test.etag, *w.uploadedParts[0].ETag)
			}
			assert.Equal(t, int32(1), uploadPartCalls.Load())
			assert.Equal(t, int32(1), listPartsCalls.Load())
		})
	}
}
