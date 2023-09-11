package api

import (
	"encoding/json"
	"strconv"
	"time"
)

type BaseResponse struct {
	Errno interface{} `json:"errno"`
	Error string      `json:"error,omitempty"`
	State bool        `json:"state"`
}

type UploadInfoResponse struct {
	Error       string      `json:"error,omitempty"`
	State       bool        `json:"state"`
	AppID       json.Number `json:"app_id"`
	AppVersion  json.Number `json:"app_version"`
	UploadLimit int64       `json:"size_limit"`
	IspType     int64       `json:"isp_type"`
	UserID      int64       `json:"user_id"`
	UserKey     string      `json:"userkey"`
}

type UploadInitResponse struct {
	Request   string `json:"request"`
	ErrorCode int    `json:"statuscode"`
	ErrorMsg  string `json:"statusmsg"`

	Status   json.Number `json:"status"`
	PickCode string      `json:"pickcode"`

	// OSS upload fields
	Bucket   string `json:"bucket"`
	Object   string `json:"object"`
	Callback struct {
		Callback    string `json:"callback"`
		CallbackVar string `json:"callback_var"`
	} `json:"callback"`

	// Useless fields
	FileID   int    `json:"fileid"`
	FileInfo string `json:"fileinfo"`
	Target   string `json:"target"`
}

type UploadOssTokenResponse struct {
	StatusCode      string `json:"StatusCode"`
	AccessKeyID     string `json:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken"`
	Expiration      string `json:"Expiration"`
}

type GetFilesResponse struct {
	AreaID     string      `json:"aid"`
	CategoryID json.Number `json:"cid"`
	Count      int64       `json:"count"`
	Cur        int64       `json:"cur"`
	Data       []FileInfo  `json:"data"`
	DataSource string      `json:"data_source"`
	Errno      int64       `json:"errNo"`
	Error      string      `json:"error"`
	Limit      int64       `json:"limit"`
	MaxSize    int64       `json:"max_size"`
	MinSize    int64       `json:"min_size"`
	Offset     int64       `json:"offset"`
	Order      string      `json:"order"`
	PageSize   int64       `json:"page_size"`
	Path       []FileInfo  `json:"path"`
	State      bool        `json:"state"`
	Suffix     string      `json:"suffix"`
}

type GetDirIDResponse struct {
	Errno      json.Number `json:"errno"`
	Error      string      `json:"error"`
	CategoryID json.Number `json:"id"`
	IsPrivate  json.Number `json:"is_private"`
	State      bool        `json:"state"`
}

type IndexInfoResponse struct {
	Error string        `json:"error,omitempty"`
	State bool          `json:"state"`
	Data  IndexInfoData `json:"data"`
}

type IndexInfoData struct {
	SpaceInfo map[string]SizeInfo `json:"space_info"`
}

type SizeInfo struct {
	Size       float64 `json:"size"`
	SizeFormat string  `json:"size_format"`
}

type GetURLResponse struct {
	State bool            `json:"state"`
	Msg   string          `json:"msg"`
	Errno json.Number     `json:"errno"`
	Error string          `json:"error,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

type MkdirResponse struct {
	Errno interface{} `json:"errno"`
	Error string      `json:"error,omitempty"`
	State bool        `json:"state"`
}

type DownloadURL struct {
	URL    string      `json:"url"`
	Client json.Number `json:"client"`
	Desc   string      `json:"desc"`
	OssID  string      `json:"oss_id"`
}

type DownloadInfo struct {
	FileName string      `json:"file_name"`
	FileSize json.Number `json:"file_size"`
	PickCode string      `json:"pick_code"`
	URL      DownloadURL `json:"url"`
}

type DownloadData map[string]*DownloadInfo

type FileInfo struct {
	AreaID     json.Number `json:"aid"`
	CategoryID json.Number `json:"cid"`
	FileID     json.Number `json:"fid"`
	ParentID   json.Number `json:"pid"`

	Name     string      `json:"n"`
	Type     string      `json:"ico"`
	Size     json.Number `json:"s"`
	Sha1     string      `json:"sha"`
	PickCode string      `json:"pc"`

	CreateTime json.Number `json:"tp"`
	UpdateTime json.Number `json:"te"`
}

func (f *FileInfo) GetName() string {
	return f.Name
}

func (f *FileInfo) GetSize() int64 {
	size, _ := f.Size.Int64()
	return size
}

func (f *FileInfo) GetUpdateTime() time.Time {
	updateTime, _ := f.UpdateTime.Int64()
	return time.Unix(updateTime, 0).UTC()
}

func (f *FileInfo) GetCreateTime() time.Time {
	updateTime, _ := f.UpdateTime.Int64()
	return time.Unix(updateTime, 0).UTC()
}

func (f *FileInfo) IsDir() bool {
	return f.GetFileID() == 0
}

func (f *FileInfo) GetFileID() int64 {
	fid, _ := f.FileID.Int64()
	return fid
}

func (f *FileInfo) GetCategoryID() int64 {
	cid, _ := f.CategoryID.Int64()
	return cid
}

func (f *FileInfo) GetParentID() int64 {
	pid, _ := f.ParentID.Int64()
	return pid
}

func (r *MkdirResponse) GetErrno() int64 {
	if val, ok := r.Errno.(string); ok {
		errno, _ := strconv.ParseInt(val, 10, 64)
		return errno
	}
	if val, ok := r.Errno.(int64); ok {
		return val
	}
	if val, ok := r.Errno.(float64); ok {
		return int64(val)
	}

	return 0
}
