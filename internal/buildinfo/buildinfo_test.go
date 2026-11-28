package buildinfo

import (
	"runtime/debug"
	"testing"
)

func TestResolveUsesLinkTimeValues(t *testing.T) {
	bi := &debug.BuildInfo{
		Main: debug.Module{Version: "v9.9.9"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "from-vcs"},
			{Key: "vcs.modified", Value: "true"},
			{Key: "vcs.time", Value: "2000-01-01T00:00:00Z"},
		},
	}
	got := Resolve("v1.2.3", "deadbeef", "2026-11-27T00:00:00Z", bi)
	want := Info{Version: "v1.2.3", Commit: "deadbeef", Date: "2026-11-27T00:00:00Z"}
	if got != want {
		t.Fatalf("Resolve() = %+v, want %+v", got, want)
	}
	if line := Format("compute-control-panel", got); line != "compute-control-panel v1.2.3 (commit deadbeef, built 2026-11-27T00:00:00Z)" {
		t.Fatalf("Format() = %q", line)
	}
}

func TestResolveFallsBackToBuildInfo(t *testing.T) {
	bi := &debug.BuildInfo{
		Main: debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abc123"},
			{Key: "vcs.modified", Value: "true"},
			{Key: "vcs.time", Value: "2020-01-01T00:00:00Z"},
			{Key: "GOARCH", Value: "amd64"},
		},
	}
	got := Resolve("", "", "", bi)
	want := Info{Version: "(devel)", Commit: "abc123", Dirty: true}
	if got != want {
		t.Fatalf("Resolve() = %+v, want %+v", got, want)
	}
	if line := Format("ccp-demo", got); line != "ccp-demo (devel) (commit abc123, dirty, built unknown)" {
		t.Fatalf("Format() = %q", line)
	}
}

func TestResolvePartialFallbackAndCleanRevision(t *testing.T) {
	bi := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.0.0"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "fff"},
			{Key: "vcs.modified", Value: "false"},
		},
	}
	got := Resolve("v1.0.0", "", "2026-01-02T03:04:05Z", bi)
	want := Info{Version: "v1.0.0", Commit: "fff", Date: "2026-01-02T03:04:05Z"}
	if got != want {
		t.Fatalf("Resolve() = %+v, want %+v", got, want)
	}
}

func TestResolveNilBuildInfo(t *testing.T) {
	got := Resolve("", "", "", nil)
	if got != (Info{}) {
		t.Fatalf("Resolve() = %+v, want zero Info", got)
	}
	if line := Format("migrate", got); line != "migrate unknown (commit unknown, built unknown)" {
		t.Fatalf("Format() = %q", line)
	}
}

func TestCurrentMatchesResolve(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("ReadBuildInfo unavailable")
	}
	if got, want := Current(), Resolve(Version, Commit, Date, bi); got != want {
		t.Fatalf("Current() = %+v, want %+v", got, want)
	}
}

func TestVersionRequested(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"nil", nil, false},
		{"empty", []string{}, false},
		{"short", []string{"-version"}, true},
		{"long", []string{"--version"}, true},
		{"among flags", []string{"-addr", "127.0.0.1:8081", "-version"}, true},
		{"after end of flags", []string{"--", "-version"}, false},
		{"value", []string{"-version=true"}, false},
		{"prefix", []string{"-v"}, false},
		{"operand", []string{"version"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := VersionRequested(tc.args); got != tc.want {
				t.Fatalf("VersionRequested(%q) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}
