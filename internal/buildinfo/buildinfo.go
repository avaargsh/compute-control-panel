// Package buildinfo reports the version, commit, and build date embedded in a
// user-facing binary.
//
// Version, Commit, and Date are empty unless set at link time:
//
//	-X github.com/avaargsh/compute-control-panel/internal/buildinfo.Version=<version>
//	-X github.com/avaargsh/compute-control-panel/internal/buildinfo.Commit=<commit>
//	-X github.com/avaargsh/compute-control-panel/internal/buildinfo.Date=<date>
//
// Empty fields fall back to runtime/debug.ReadBuildInfo: the main module
// version, vcs.revision, and vcs.modified. The build date has no build-info
// equivalent and stays empty until -ldflags sets it.
package buildinfo

import (
	"fmt"
	"runtime/debug"
)

// Version, Commit, and Date are overridden with -ldflags -X.
var (
	Version = ""
	Commit  = ""
	Date    = ""
)

// Info is the resolved build identity. Dirty is true only when Commit was
// taken from vcs.revision and vcs.modified is "true".
type Info struct {
	Version string
	Commit  string
	Date    string
	Dirty   bool
}

// Current resolves the link-time values, using ReadBuildInfo for empty fields.
func Current() Info {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		bi = nil
	}
	return Resolve(Version, Commit, Date, bi)
}

// Resolve fills empty link-time values from bi. A nil bi leaves those fields
// empty. Non-empty link-time values are kept as set.
func Resolve(version, commit, date string, bi *debug.BuildInfo) Info {
	info := Info{Version: version, Commit: commit, Date: date}
	if bi == nil {
		return info
	}
	if info.Version == "" {
		info.Version = bi.Main.Version
	}
	if info.Commit == "" {
		revision, modified := vcsRevision(bi)
		info.Commit = revision
		info.Dirty = modified
	}
	return info
}

func vcsRevision(bi *debug.BuildInfo) (revision string, modified bool) {
	for _, setting := range bi.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return revision, modified
}

// Line is the one-line report for binary using the current build identity.
func Line(binary string) string {
	return Format(binary, Current())
}

// Format renders "<binary> <version> (commit <sha>[, dirty], built <date>)".
// Empty version, commit, and date render as "unknown".
func Format(binary string, info Info) string {
	version := info.Version
	if version == "" {
		version = "unknown"
	}
	commit := info.Commit
	if commit == "" {
		commit = "unknown"
	}
	date := info.Date
	if date == "" {
		date = "unknown"
	}
	dirty := ""
	if info.Dirty {
		dirty = ", dirty"
	}
	return fmt.Sprintf("%s %s (commit %s%s, built %s)", binary, version, commit, dirty, date)
}

// VersionRequested reports whether args select -version or --version before a
// "--" end-of-flags marker. Other argument forms are left untouched.
func VersionRequested(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if arg == "-version" || arg == "--version" {
			return true
		}
	}
	return false
}
