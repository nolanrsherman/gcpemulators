package gcpemulators

import (
	_ "embed"
	"strings"
)

// embeddedVersion holds the contents of version.txt embedded at build time.
//
//go:embed version.txt
var embeddedVersion string

// Version returns the trimmed version string embedded from version.txt.
func Version() string {
	v := strings.TrimSpace(embeddedVersion)
	if v == "" {
		return "unknown"
	}
	return v
}
