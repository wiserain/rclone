package pikpak

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/rclone/rclone/fs"
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
