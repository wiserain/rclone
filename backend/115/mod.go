package _115

import (
	"fmt"
	"regexp"
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
	return
}
