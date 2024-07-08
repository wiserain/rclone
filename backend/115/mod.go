package _115

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
	re := regexp.MustCompile(`^\{(\d{19})\}`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return "", fmt.Errorf("%s does not contain a valid id", s)
	}
	rootID = m[1]
	return
}

// get an id of file or directory
func (f *Fs) getID(ctx context.Context, path string) (id string, err error) {
	if id, _ := parseRootID(path); len(id) > 6 {
		info, err := f.getFile(ctx, id)
		if err != nil {
			return "", fmt.Errorf("no such object with id %q: %w", id, err)
		}
		return info.ID(), nil
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
