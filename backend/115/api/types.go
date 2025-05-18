package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

// String ensures JSON unmarshals to a string, handling both quoted and unquoted inputs.
// Unquoted inputs are treated as raw bytes and converted directly to a string.
type String string

func (s *String) UnmarshalJSON(in []byte) error {
	if n := len(in); n > 1 && in[0] == '"' && in[n-1] == '"' {
		return json.Unmarshal(in, (*string)(s))
	}
	*s = String(in)
	return nil
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
	Msg     string `json:"msg,omitempty"`
	Errno   Int    `json:"errno,omitempty"`   // Base, NewDir, DirID, UploadBasicInfo, ShareSnap
	ErrNo   Int    `json:"errNo,omitempty"`   // FileList
	Code    Int    `json:"code,omitempty"`    // FileInfo, CallbackInfo
	Error   string `json:"error,omitempty"`   // Base, FileList, NewDir, DirID, UploadBasicInfo, ShareSnap
	Message string `json:"message,omitempty"` // FileInfo, CallbackInfo
	State   bool   `json:"state,omitempty"`
}

func (b *Base) ErrCode() Int {
	if b.Errno != 0 {
		return b.Errno
	}
	if b.ErrNo != 0 {
		return b.ErrNo
	}
	return b.Code
}

func (b *Base) ErrMsg() string {
	if b.Error != "" {
		return b.Error
	}
	if b.Message != "" {
		return b.Message
	}
	return b.Msg
}

// Returnes Error or Nil
func (b *Base) Err() error {
	if b.State {
		return nil
	}
	out := fmt.Sprintf("API Error(%d)", b.ErrCode())
	if msg := b.ErrMsg(); msg != "" {
		out += fmt.Sprintf(": %q", msg)
	}
	return errors.New(out)
}

type File struct {
	FID       string      `json:"fid,omitempty"` // file; empty if dir
	UID       json.Number `json:"uid,omitempty"` // user
	AID       json.Number `json:"aid,omitempty"` // area
	CID       json.Number `json:"cid,omitempty"` // category == directory
	PID       string      `json:"pid,omitempty"` // parent
	Name      string      `json:"n,omitempty"`
	Size      Int64       `json:"s,omitempty"`
	PickCode  string      `json:"pc,omitempty"`
	T         string      `json:"t,omitempty"`  // representative time? "2024-05-19 03:54" or "1715919337"
	Te        Time        `json:"te,omitempty"` // modify time
	Tp        Time        `json:"tp,omitempty"` // create time
	Tu        Time        `json:"tu,omitempty"` // update time?
	To        Time        `json:"to,omitempty"` // last opened 0 if never accessed or "1716165082"
	Ico       string      `json:"ico,omitempty"`
	Class     string      `json:"class,omitempty"`
	Sha       string      `json:"sha,omitempty"`
	CheckCode int         `json:"check_code,omitempty"`
	CheckMsg  string      `json:"check_msg,omitempty"`
	Score     Int         `json:"score,omitempty"`
	PlayLong  float64     `json:"play_long,omitempty"` // playback secs if media
	Censored  int         `json:"c,omitempty"`
	// c=1 文件内含违规内容 file contains prohibited content
	// c=2 系统处理中，暂不支持操作 system processing; operation not supported at this time
}

func (f *File) IsDir() bool {
	return f.FID == ""
}

func (f *File) ID() string {
	if f.IsDir() {
		return f.CID.String()
	}
	return f.FID
}

func (f *File) ParentID() string {
	if f.IsDir() {
		return f.PID
	}
	return f.CID.String()
}

func (f *File) ModTime() time.Time {
	if t := time.Time(f.Te); !t.IsZero() {
		return t
	}
	if t := time.Time(f.Tu); !t.IsZero() {
		return t
	}
	// file object in ShareSnap.Data.List[] has T field only
	if ts, err := strconv.ParseInt(f.T, 10, 64); err == nil {
		return time.Unix(ts, 0)
	}
	return time.Time{}
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
	Base
	Files          []*File     `json:"data,omitempty"`
	Count          int         `json:"count,omitempty"`
	DataSource     string      `json:"data_source,omitempty"`
	SysCount       int         `json:"sys_count,omitempty"`
	FileCount      int         `json:"file_count,omitempty"`
	FolderCount    int         `json:"folder_count,omitempty"`
	PageSize       int         `json:"page_size,omitempty"`
	AID            string      `json:"aid,omitempty"`
	CID            json.Number `json:"cid,omitempty"`
	IsAsc          json.Number `json:"is_asc,omitempty"`
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
}

type FileInfo struct {
	Base
	Data []*File `json:"data,omitempty"`
}

type NewDir struct {
	Base
	AID      int    `json:"aid,omitempty"`
	CID      string `json:"cid,omitempty"`
	Cname    string `json:"cname,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
}

type DirID struct {
	Base
	ID        json.Number `json:"id,omitempty"`
	IsPrivate json.Number `json:"is_private,omitempty"`
}

type FileStats struct {
	Count        json.Number `json:"count,omitempty"`
	Size         string      `json:"size,omitempty"`
	FolderCount  json.Number `json:"folder_count,omitempty"`
	ShowPlayLong int         `json:"show_play_long,omitempty"`
	PlayLong     int         `json:"play_long,omitempty"`
	Ptime        string      `json:"ptime,omitempty"` // create time
	Utime        string      `json:"utime,omitempty"` // update time?
	IsShare      string      `json:"is_share,omitempty"`
	FileName     string      `json:"file_name,omitempty"`
	PickCode     string      `json:"pick_code,omitempty"`
	Sha1         string      `json:"sha1,omitempty"`
	IsMark       string      `json:"is_mark,omitempty"`
	Fvs          int         `json:"fvs,omitempty"`
	OpenTime     int         `json:"open_time,omitempty"` // last opened
	Score        Int         `json:"score,omitempty"`
	Desc         string      `json:"desc,omitempty"`
	FileCategory string      `json:"file_category,omitempty"` // "0" if dir
	Paths        []struct {
		FileID   json.Number `json:"file_id,omitempty"`
		FileName string      `json:"file_name,omitempty"`
	} `json:"paths,omitempty"`
}

type StringInfo struct {
	Base
	Data String `json:"data,omitempty"`
}

type IndexInfo struct {
	Base
	Data *IndexData `json:"data,omitempty"`
}

type IndexData struct {
	SpaceInfo map[string]*SizeInfo `json:"space_info"`
}

type SizeInfo struct {
	Size       float64 `json:"size"`
	SizeFormat string  `json:"size_format"`
}

// ------------------------------------------------------------

type DownloadURL struct {
	URL     string `json:"url"`
	Client  Int    `json:"client"`
	Desc    string `json:"desc"`
	OssID   string `json:"oss_id"`
	Cookies []*http.Cookie
}

func (u *DownloadURL) UnmarshalJSON(data []byte) error {
	if string(data) == "false" {
		*u = DownloadURL{}
		return nil
	}

	type Alias DownloadURL // Use type alias to avoid recursion
	aux := Alias{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*u = DownloadURL(aux)
	return nil
}

// expiry parses expiry from URL parameter t
func (u *DownloadURL) expiry() time.Time {
	if p, err := url.Parse(u.URL); err == nil {
		if q, err := url.ParseQuery(p.RawQuery); err == nil {
			if t := q.Get("t"); t != "" {
				if i, err := strconv.ParseInt(t, 10, 64); err == nil {
					return time.Unix(i, 0)
				}
			}
		}
	}
	return time.Time{}
}

// expired reports whether the token is expired.
// u must be non-nil.
func (u *DownloadURL) expired() bool {
	expiry := u.expiry()
	if expiry.IsZero() {
		return false
	}

	expiryDelta := time.Duration(10) * time.Second
	return expiry.Round(0).Add(-expiryDelta).Before(time.Now())
}

// Valid reports whether u is non-nil and is not expired.
func (u *DownloadURL) Valid() bool {
	return u != nil && !u.expired()
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
	Base
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
}

type UploadInitInfo struct {
	Request   string `json:"request"`
	ErrorCode int    `json:"statuscode"`
	ErrorMsg  string `json:"statusmsg"`

	Status   Int    `json:"status"`
	PickCode string `json:"pickcode"` // valid depending on Status
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

func (ui *UploadInitInfo) GetCallback() string {
	return base64.StdEncoding.EncodeToString([]byte(ui.Callback.Callback))
}

func (ui *UploadInitInfo) GetCallbackVar() string {
	return base64.StdEncoding.EncodeToString([]byte(ui.Callback.CallbackVar))
}

type CallbackInfo struct {
	Base
	Data *CallbackData `json:"data,omitempty"`
}

type CallbackData struct {
	AID      int    `json:"aid,omitempty"`
	CID      string `json:"cid,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
	FileSize Int64  `json:"file_size,omitempty"`
	IsVideo  int    `json:"is_video,omitempty"`
	PickCode string `json:"pick_code,omitempty"`
	Sha      string `json:"sha1,omitempty"`
	ThumbURL string `json:"thumb_url,omitempty"`
}

type OSSToken struct {
	AccessKeyID     string    `json:"AccessKeyID"`
	AccessKeySecret string    `json:"AccessKeySecret"`
	Expiration      time.Time `json:"Expiration"`
	SecurityToken   string    `json:"SecurityToken"`
	StatusCode      string    `json:"StatusCode"`
	ErrorCode       string    `json:"ErrorCode,omitempty"`
	ErrorMessage    string    `json:"ErrorMessage,omitempty"`
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

type ShareSnap struct {
	Base
	Data *ShareSnapData `json:"data,omitempty"`
}

type ShareSnapData struct {
	Userinfo struct {
		UserID   string `json:"user_id,omitempty"`
		UserName string `json:"user_name,omitempty"`
		Face     string `json:"face,omitempty"`
	} `json:"userinfo,omitempty"`
	Shareinfo struct {
		SnapID           string      `json:"snap_id,omitempty"`
		FileSize         string      `json:"file_size,omitempty"`
		ShareTitle       string      `json:"share_title,omitempty"`
		ShareState       json.Number `json:"share_state,omitempty"`
		ForbidReason     string      `json:"forbid_reason,omitempty"`
		CreateTime       string      `json:"create_time,omitempty"`
		ReceiveCode      string      `json:"receive_code,omitempty"`
		ReceiveCount     string      `json:"receive_count,omitempty"`
		ExpireTime       int         `json:"expire_time,omitempty"`
		FileCategory     int         `json:"file_category,omitempty"`
		AutoRenewal      string      `json:"auto_renewal,omitempty"`
		AutoFillRecvcode string      `json:"auto_fill_recvcode,omitempty"`
		CanReport        int         `json:"can_report,omitempty"`
		CanNotice        int         `json:"can_notice,omitempty"`
		HaveVioFile      int         `json:"have_vio_file,omitempty"`
	} `json:"shareinfo,omitempty"`
	Count      int         `json:"count,omitempty"`
	List       []*File     `json:"list,omitempty"`
	ShareState json.Number `json:"share_state,omitempty"`
	UserAppeal struct {
		CanAppeal       int `json:"can_appeal,omitempty"`
		CanShareAppeal  int `json:"can_share_appeal,omitempty"`
		PopupAppealPage int `json:"popup_appeal_page,omitempty"`
		CanGlobalAppeal int `json:"can_global_appeal,omitempty"`
	} `json:"user_appeal,omitempty"`
}

type ShareDownloadInfo struct {
	FileID   string      `json:"fid"`
	FileName string      `json:"fn"`
	FileSize Int64       `json:"fs"`
	URL      DownloadURL `json:"url"`
}
