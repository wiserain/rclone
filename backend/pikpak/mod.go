package pikpak

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/rclone/rclone/backend/pikpak/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

// ------------------------------------------------------------

// parseRootID parses RootID from path
func parseRootID(s string) (rootID string, err error) {
	re := regexp.MustCompile(`\{([^}]{5,})\}`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return "", fmt.Errorf("%s does not contain a valid id", s)
	}
	rootID = m[1]

	if strings.HasPrefix(rootID, "http") {
		// https://mypikpak.com/drive/all/{ID}
		// https://mypikpak.com/drive/recent/{ID}
		// https://mypikpak.com/s/{ID}
		re := regexp.MustCompile(`\/drive\/(all|recent)(\/([A-Za-z0-9_-]{6,}))+\/?`)
		if m := re.FindStringSubmatch(rootID); m != nil {
			rootID = m[len(m)-1]
			return
		}
	}
	return
}

// get an id of file or directory
func (f *Fs) getID(ctx context.Context, path string) (id string, err error) {
	if id, _ := parseRootID(path); len(id) > 6 {
		info, err := f.getFile(ctx, id)
		if err != nil {
			return "", fmt.Errorf("no such object with id %q: %w", id, err)
		}
		return info.ID, nil
	}
	path = strings.Trim(path, "/")
	id, err = f.dirCache.FindDir(ctx, path, false)
	if err != nil {
		o, err := f.NewObject(ctx, path)
		if err != nil {
			return "", err
		}
		id = o.(fs.IDer).ID()
	}
	return id, nil
}

type RedeemResult struct {
	AddDays int    `json:"add_days"`
	Code    string `json:"code"`
	Data    struct {
		Expire api.Time `json:"expire"`
		Status string   `json:"status"`
		Type   string   `json:"type"`
		UserID string   `json:"user_id"`
	} `json:"data"`
	FreeDays    int `json:"free_days"`
	InvitedDays int `json:"invited_days"`
	Popup       struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Right string `json:"right"`
		Title struct {
			Text  string `json:"text"`
			Color string `json:"color"`
		} `json:"title"`
		Description struct {
			Text  string `json:"text"`
			Color string `json:"color"`
		} `json:"description"`
		Image  string `json:"image"`
		Button struct {
			Text struct {
				Text  string `json:"text"`
				Color string `json:"color"`
			} `json:"text"`
			Color    string `json:"color"`
			DeepLink string `json:"deepLink"`
		} `json:"button"`
		SecondaryButton interface{} `json:"secondaryButton"`
		Icon            interface{} `json:"icon"`
	} `json:"popup"`
	Result  string `json:"result"`
	Updated bool   `json:"updated"`
}

// requestRedeem requests for redeem
func (f *Fs) requestRedeem(ctx context.Context, code string) (info *RedeemResult, err error) {
	req := struct {
		ActivationCode string `json:"activation_code"`
		Data           struct {
		} `json:"data,omitempty"`
	}{
		ActivationCode: code,
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/vip/v1/order/activation-code",
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.rst.CallJSON(ctx, &opts, &req, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	return
}

// ResourceList contains a list of Resource elements
type ResourceList struct {
	ListID string `json:"list_id,omitempty"`
	List   struct {
		PageSize      int         `json:"page_size,omitempty"`
		NextPageToken string      `json:"next_page_token,omitempty"`
		Resources     []*Resource `json:"resources,omitempty"`
	} `json:"list"`
}

type ResourceMeta struct {
	Error         string `json:"error,omitempty"` // "403:E_PARSE_BT"
	Icon          string `json:"icon,omitempty"`
	Status        string `json:"status,omitempty"` // "1" if cached?
	ThumbnailLink string `json:"thumbnail_link,omitempty"`
	URL           string `json:"url,omitempty"`
}

type ResourceDir struct {
	PageSize      int         `json:"page_size"`
	NextPageToken string      `json:"next_page_token"`
	Resources     []*Resource `json:"resources,omitempty"`
}

// Resource is a basic element of resources in server
type Resource struct {
	ID        string        `json:"id,omitempty"`
	Name      string        `json:"name"`
	FileSize  int64         `json:"file_size,string"`
	FileCount int           `json:"file_count"`
	FileIndex int           `json:"file_index"`
	Meta      *ResourceMeta `json:"meta"`
	IsDir     bool          `json:"is_dir"`
	Dir       *ResourceDir  `json:"dir,omitempty"`
	ParentID  string        `json:"parent_id,omitempty"`
	Resolver  string        `json:"resolver,omitempty"` // "DIRECT"
	// custom fields
	IsCached  bool      `json:"is_cached"`
	Task      *api.Task `json:"task,omitempty"`
	TaskAdded bool      `json:"task_added"`
}

func (r *Resource) checkCached() {
	if r.Meta.Error == "" && r.Meta.Status == "1" {
		r.IsCached = true
	}
}

// requestRedeem requests for redeem
func (f *Fs) requestResourceList(ctx context.Context, urls []string) (info *ResourceList, err error) {
	req := struct {
		URLs          string `json:"urls"`
		PageSize      int    `json:"page_size"`
		ThumbnailType string `json:"thumbnail_type"`
	}{
		URLs:          strings.Join(urls, " "),
		PageSize:      500,
		ThumbnailType: "FROM_HASH",
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/drive/v1/resource/list",
	}
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.rst.CallJSON(ctx, &opts, &req, &info)
		return f.shouldRetry(ctx, resp, err)
	})
	return
}
