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
func parseRootID(s string) (rootID, receiveCode string, err error) {
	re := regexp.MustCompile(`^\{([^}]{11,})\}`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return "", "", fmt.Errorf("%s does not contain a valid id", s)
	}
	rootID = m[1]

	re = regexp.MustCompile(`(cid=)?(\d{19})`)
	if m := re.FindStringSubmatch(rootID); m != nil {
		// https://115.com/?cid={CID}&offset=0&tab=&mode=wangpan
		// {CID}
		return m[len(m)-1], "", nil
	}
	// Assume it is a share link
	if sCode, rCode, _ := parseShareLink(rootID); len(sCode) == 11 {
		return sCode, rCode, nil
	}
	return "", "", nil
}

// get an id of file or directory
func (f *Fs) getID(ctx context.Context, path string) (id string, err error) {
	if id, _, _ := parseRootID(path); len(id) == 19 {
		info, err := f.getFile(ctx, id, "")
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
