package buildinfo

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func TestBuildDateScript(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("the build-date script targets Linux and macOS")
	}
	for _, tc := range []struct {
		epoch string
		want  string
	}{
		{"0", "1970-01-01T00:00:00Z"},
		{"000001234", "1970-01-01T00:20:34Z"},
		{"1700000000", "2023-11-14T22:13:20Z"},
		{"invalid", ""},
	} {
		t.Run(tc.epoch, func(t *testing.T) {
			t.Setenv("SOURCE_DATE_EPOCH", tc.epoch)
			out, err := exec.Command("sh", "../../scripts/build-date.sh").CombinedOutput()
			if tc.want == "" {
				if err == nil || !strings.Contains(string(out), "must be a non-negative integer") {
					t.Fatalf("invalid epoch = %q, %v", out, err)
				}
				return
			}
			if err != nil || strings.TrimSpace(string(out)) != tc.want {
				t.Fatalf("timestamp = %q, %v; want %q", out, err, tc.want)
			}
		})
	}
}
