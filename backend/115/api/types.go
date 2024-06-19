package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Time represents date and time information
type Time time.Time

// MarshalJSON turns a Time into JSON (in UTC)
func (t *Time) MarshalJSON() (out []byte, err error) {
	s := strconv.Itoa(int((*time.Time)(t).Unix()))
	return []byte(s), nil
}

// UnmarshalJSON turns JSON into a Time
func (t *Time) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	if s == "null" || s == "" {
		return nil
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	newT := time.Unix(i, 0)
	*t = Time(newT)
	return nil
}

type Data struct {
	IndexInfo   *IndexInfo
	EncodedData string
}

func (d *Data) UnmarshalJSON(in []byte) (err error) {
	idx, str := IndexInfo{}, ""
	if err = json.Unmarshal(in, &idx); err == nil {
		d.IndexInfo = &idx
		return
	}
	if err = json.Unmarshal(in, &str); err == nil {
		d.EncodedData = str
		return
	}
	return
}

type Int int

func (e *Int) UnmarshalJSON(in []byte) (err error) {
	s := strings.Trim(string(in), `"`)
	if s == "" {
		s = "0"
	}
	if i, err := strconv.Atoi(s); err == nil {
		*e = Int(i)
	}
	return
}

type Int64 int64

func (e *Int64) UnmarshalJSON(in []byte) (err error) {
	s := strings.Trim(string(in), `"`)
	if s == "" {
		s = "0"
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		*e = Int64(i)
	}
	return
}

type Error struct {
	Status    int    `json:"status,omitempty"`
	Message   string `json:"message,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// Error returns a string for the error and satisfies the error interface
func (e *Error) Error() string {
	out := fmt.Sprintf("Status %d %q", e.Status, e.Message)
	if e.RequestID != "" {
		out += ": " + e.RequestID
	}
	return out
}

// Check Error satisfies the error interface
var _ error = (*Error)(nil)

// ------------------------------------------------------------

type Base struct {
	Msg   string `json:"msg,omitempty"`
	Errno Int    `json:"errno,omitempty"`
	Error string `json:"error,omitempty"`
	State bool   `json:"state,omitempty"`
	Data  Data   `json:"data,omitempty"`
}

type File struct {
	FID       string      `json:"fid,omitempty"` // file; empty if dir
	UID       json.Number `json:"uid,omitempty"` // user
	AID       json.Number `json:"aid,omitempty"` // area
	CID       string      `json:"cid,omitempty"` // category == directory
	PID       string      `json:"pid,omitempty"` // parent
	Name      string      `json:"n,omitempty"`
	Size      int64       `json:"s,omitempty"`
	PickCode  string      `json:"pc,omitempty"`
	T         string      `json:"t,omitempty"`  // mtime "2024-05-19 03:54" or "1715919337"
	Te        Time        `json:"te,omitempty"` // mtime
	Tp        Time        `json:"tp,omitempty"` // ctime
	Tu        Time        `json:"tu,omitempty"` // mtime
	To        Time        `json:"to,omitempty"` // atime 0 if never accessed or "1716165082"
	Ico       string      `json:"ico,omitempty"`
	Class     string      `json:"class,omitempty"`
	Sha       string      `json:"sha,omitempty"`
	CheckCode int         `json:"check_code,omitempty"`
	CheckMsg  string      `json:"check_msg,omitempty"`
	Score     int         `json:"score,omitempty"`
	PlayLong  float64     `json:"play_long,omitempty"` // playback secs if media
}

func (f *File) IsDir() bool {
	return f.FID == ""
}

func (f *File) ID() string {
	if f.FID == "" {
		return f.CID
	}
	return f.FID
}

type FilePath struct {
	Name string      `json:"name,omitempty"`
	AID  json.Number `json:"aid,omitempty"` // area
	CID  json.Number `json:"cid,omitempty"` // category
	PID  json.Number `json:"pid,omitempty"` // parent
	Isp  json.Number `json:"isp,omitempty"`
	PCid string      `json:"p_cid,omitempty"`
	Iss  string      `json:"iss,omitempty"`
	Fv   string      `json:"fv,omitempty"`
	Fvs  string      `json:"fvs,omitempty"`
}

type FileList struct {
	Files          []*File     `json:"data,omitempty"`
	Count          int         `json:"count,omitempty"`
	DataSource     string      `json:"data_source,omitempty"`
	SysCount       int         `json:"sys_count,omitempty"`
	FileCount      int         `json:"file_count,omitempty"`
	FolderCount    int         `json:"folder_count,omitempty"`
	PageSize       int         `json:"page_size,omitempty"`
	AID            string      `json:"aid,omitempty"`
	CID            json.Number `json:"cid,omitempty"`
	IsAsc          int         `json:"is_asc,omitempty"`
	Star           int         `json:"star,omitempty"`
	IsShare        int         `json:"is_share,omitempty"`
	Type           int         `json:"type,omitempty"`
	IsQ            int         `json:"is_q,omitempty"`
	RAll           int         `json:"r_all,omitempty"`
	Stdir          int         `json:"stdir,omitempty"`
	Cur            int         `json:"cur,omitempty"`
	MinSize        int         `json:"min_size,omitempty"`
	MaxSize        int         `json:"max_size,omitempty"`
	RecordOpenTime string      `json:"record_open_time,omitempty"`
	Path           []*FilePath `json:"path,omitempty"`
	Fields         string      `json:"fields,omitempty"`
	Order          string      `json:"order,omitempty"`
	FcMix          int         `json:"fc_mix,omitempty"`
	Natsort        int         `json:"natsort,omitempty"`
	UID            json.Number `json:"uid,omitempty"`
	Offset         int         `json:"offset,omitempty"`
	Limit          int         `json:"limit,omitempty"`
	Suffix         string      `json:"suffix,omitempty"`
	State          bool        `json:"state,omitempty"`
	Error          string      `json:"error,omitempty"`
	ErrNo          int         `json:"errNo,omitempty"`
}

type FileInfo struct {
	State   bool    `json:"state,omitempty"`
	Code    Int     `json:"code,omitempty"`
	Message string  `json:"message,omitempty"`
	Data    []*File `json:"data,omitempty"`
}

type NewDir struct {
	State    bool   `json:"state,omitempty"`
	Error    string `json:"error,omitempty"`
	Errno    Int    `json:"errno,omitempty"`
	AID      int    `json:"aid,omitempty"`
	CID      string `json:"cid,omitempty"`
	Cname    string `json:"cname,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
}

type DirID struct {
	State     bool        `json:"state,omitempty"`
	Error     string      `json:"error,omitempty"`
	Errno     Int         `json:"errno,omitempty"`
	ID        json.Number `json:"id,omitempty"`
	IsPrivate json.Number `json:"is_private,omitempty"`
}

type FileStats struct {
	// Count        json.Number `json:"count,omitempty"`
	// Size         string      `json:"size,omitempty"`
	// FolderCount  json.Number `json:"folder_count,omitempty"`
	// ShowPlayLong int         `json:"show_play_long,omitempty"`
	// PlayLong     int         `json:"play_long,omitempty"`
	// Ptime        string      `json:"ptime,omitempty"` // ctime
	// Utime        string      `json:"utime,omitempty"` // mtime
	// IsShare      string      `json:"is_share,omitempty"`
	// PickCode     string      `json:"pick_code,omitempty"`
	// Sha1         string      `json:"sha1,omitempty"`
	// IsMark       string      `json:"is_mark,omitempty"`
	// Fvs          int         `json:"fvs,omitempty"`
	// OpenTime     int         `json:"open_time,omitempty"` // atime
	// Score        int         `json:"score,omitempty"`
	// Desc         string      `json:"desc,omitempty"`
	// FileCategory string      `json:"file_category,omitempty"` // "0" if dir
	FileName string `json:"file_name,omitempty"`
	Paths    []struct {
		FileID   json.Number `json:"file_id,omitempty"`
		FileName string      `json:"file_name,omitempty"`
	} `json:"paths,omitempty"`
}

type IndexInfo struct {
	SpaceInfo map[string]SizeInfo `json:"space_info"`
}

type SizeInfo struct {
	Size       float64 `json:"size"`
	SizeFormat string  `json:"size_format"`
}

// ------------------------------------------------------------

type DownloadURL struct {
	URL        string `json:"url"`
	Client     Int    `json:"client"`
	Desc       string `json:"desc"`
	OssID      string `json:"oss_id"`
	Cookies    []*http.Cookie
	CreateTime time.Time
}

// Valid reports whether u is non-nil, has an URL, and is not expired.
func (u *DownloadURL) Valid() bool {
	return u != nil && u.URL != "" && time.Since(u.CreateTime) < 100*time.Second
	// TODO: how sure for 100s expiry
}

func (u *DownloadURL) Cookie() string {
	cookie := ""
	for _, ck := range u.Cookies {
		cookie += fmt.Sprintf("%s=%s;", ck.Name, ck.Value)
	}
	return cookie
}

type DownloadInfo struct {
	FileName string      `json:"file_name"`
	FileSize Int64       `json:"file_size"`
	PickCode string      `json:"pick_code"`
	URL      DownloadURL `json:"url"`
}

type DownloadData map[string]*DownloadInfo

// ------------------------------------------------------------

type UploadBasicInfo struct {
	Uploadinfo       string      `json:"uploadinfo,omitempty"`
	UserID           json.Number `json:"user_id,omitempty"`
	AppVersion       int         `json:"app_version,omitempty"`
	AppID            int         `json:"app_id,omitempty"`
	Userkey          string      `json:"userkey,omitempty"`
	SizeLimit        int64       `json:"size_limit,omitempty"`
	SizeLimitYun     int64       `json:"size_limit_yun,omitempty"`
	MaxDirLevel      int64       `json:"max_dir_level,omitempty"`
	MaxDirLevelYun   int64       `json:"max_dir_level_yun,omitempty"`
	MaxFileNum       int64       `json:"max_file_num,omitempty"`
	MaxFileNumYun    int64       `json:"max_file_num_yun,omitempty"`
	UploadAllowed    bool        `json:"upload_allowed,omitempty"`
	UploadAllowedMsg string      `json:"upload_allowed_msg,omitempty"`
	State            bool        `json:"state,omitempty"`
	Error            string      `json:"error,omitempty"`
	Errno            Int         `json:"errno,omitempty"`
}

type UploadInitInfo struct {
	Request   string `json:"request"`
	ErrorCode int    `json:"statuscode"`
	ErrorMsg  string `json:"statusmsg"`

	Status   Int    `json:"status"`
	PickCode string `json:"pickcode"` // this pickcode is not the same as for downloading!
	Target   string `json:"target"`
	Version  string `json:"version"`

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

	// New fields in upload v4.0
	SignKey   string `json:"sign_key"`
	SignCheck string `json:"sign_check"`
}

type OSSToken struct {
	AccessKeyID     string    `json:"AccessKeyID"`
	AccessKeySecret string    `json:"AccessKeySecret"`
	Expiration      time.Time `json:"Expiration"`
	SecurityToken   string    `json:"SecurityToken"`
	StatusCode      string    `json:"StatusCode"`
}

func (t *OSSToken) TimeToExpiry() time.Duration {
	if t == nil {
		return 0
	}
	if t.Expiration.IsZero() {
		return 3e9 * time.Second // ~95 years
	}
	return time.Until(t.Expiration) - 10*time.Minute
}

// ------------------------------------------------------------

type NewURL struct {
	State    bool   `json:"state,omitempty"`
	ErrorMsg string `json:"error_msg,omitempty"`
	Errno    int    `json:"errno,omitempty"`
	Result   []struct {
		State    bool   `json:"state,omitempty"`
		ErrorMsg string `json:"error_msg,omitempty"`
		Errno    int    `json:"errno,omitempty"`
		Errtype  string `json:"errtype,omitempty"`
		Errcode  int    `json:"errcode,omitempty"`
		InfoHash string `json:"info_hash,omitempty"`
		URL      string `json:"url,omitempty"`
		Files    []struct {
			ID   string `json:"id,omitempty"`
			Name string `json:"name,omitempty"`
			Size int64  `json:"size,omitempty"`
		} `json:"files,omitempty"`
	} `json:"result,omitempty"`
	Errcode int `json:"errcode,omitempty"`
}
