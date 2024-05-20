package api

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Time represents date and time information
type Time time.Time

// MarshalJSON turns a Time into JSON (in UTC)
func (t *Time) MarshalJSON() (out []byte, err error) {
	timeString := strconv.Itoa(int((*time.Time)(t).Unix()))
	return []byte(timeString), nil
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

type Base struct {
	Errno interface{} `json:"errno"` // string or int...
	Error string      `json:"error,omitempty"`
	State bool        `json:"state"`
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
	UID            int         `json:"uid,omitempty"`
	Offset         int         `json:"offset,omitempty"`
	Limit          int         `json:"limit,omitempty"`
	Suffix         string      `json:"suffix,omitempty"`
	State          bool        `json:"state,omitempty"`
	Error          string      `json:"error,omitempty"`
	ErrNo          int         `json:"errNo,omitempty"`
}

type NewDir struct {
	State    bool   `json:"state,omitempty"`
	Error    string `json:"error,omitempty"`
	Errno    string `json:"errno,omitempty"`
	AID      int    `json:"aid,omitempty"`
	CID      string `json:"cid,omitempty"`
	Cname    string `json:"cname,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
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
