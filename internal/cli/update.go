package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// releasesURL is where update looks for the newest build.
const releasesURL = "https://api.github.com/repos/urmzd/mandatum/releases/latest"

// newUpdateCmd builds `mandatum update`, replacing the running binary in place.
//
// The replacement is written to a temporary file beside the target and then
// renamed over it. Rename within a directory is atomic, so a download that
// dies halfway leaves the existing binary untouched rather than truncated:
// the failure mode of a self-updater must never be "no working binary".
func newUpdateCmd(v Version) *cobra.Command {
	var check bool

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update mandatum to the latest release",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}

			rel, err := latestRelease(cmd.Context())
			if err != nil {
				return err
			}
			current := strings.TrimPrefix(v.Version, "v")
			latest := strings.TrimPrefix(rel.TagName, "v")

			if check {
				if u.format == FormatJSON {
					return u.json(map[string]any{
						"current": current, "latest": latest, "outdated": current != latest,
					})
				}
				if current == latest {
					u.printf("mandatum %s is current\n", current)
					return nil
				}
				u.printf("mandatum %s is available (running %s)\n", latest, current)
				return nil
			}

			if current == latest && current != "" {
				u.printf("already on %s\n", current)
				return nil
			}

			asset := assetName()
			url := ""
			for _, a := range rel.Assets {
				if a.Name == asset {
					url = a.BrowserDownloadURL
					break
				}
			}
			if url == "" {
				return fmt.Errorf("update: release %s has no asset %q for %s/%s",
					rel.TagName, asset, runtime.GOOS, runtime.GOARCH)
			}

			exe, err := os.Executable()
			if err != nil {
				return fmt.Errorf("update: locate running binary: %w", err)
			}
			if exe, err = filepath.EvalSymlinks(exe); err != nil {
				return fmt.Errorf("update: resolve running binary: %w", err)
			}

			u.logf("%s %s\n", u.dim("downloading"), url)
			if err := replaceBinary(cmd.Context(), exe, url); err != nil {
				return err
			}
			u.printf("updated to %s\n", latest)
			return nil
		},
	}

	cmd.Flags().BoolVar(&check, "check", false, "Report whether an update exists without installing it")
	return cmd
}

type release struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	} `json:"assets"`
}

func latestRelease(ctx context.Context) (*release, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, releasesURL, nil)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	res, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("update: %w: reach github: %w", errUnavailable, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("update: %w: github returned %s", errUnavailable, res.Status)
	}

	var rel release
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&rel); err != nil {
		return nil, fmt.Errorf("update: decode release: %w", err)
	}
	if rel.TagName == "" {
		return nil, fmt.Errorf("update: no released version found")
	}
	return &rel, nil
}

// assetName is the release artifact for this platform. It matches the naming
// the release workflow uses when it uploads builds.
func assetName() string {
	name := fmt.Sprintf("mandatum-%s-%s", runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return name
}

// replaceBinary downloads url and swaps it over exe atomically.
func replaceBinary(ctx context.Context, exe, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	res, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
	if err != nil {
		return fmt.Errorf("update: %w: download: %w", errUnavailable, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("update: %w: download returned %s", errUnavailable, res.Status)
	}

	// The temporary file must share a directory with the target: rename is
	// atomic only within a filesystem, and /tmp is frequently a different one.
	dir := filepath.Dir(exe)
	tmp, err := os.CreateTemp(dir, ".mandatum-update-*")
	if err != nil {
		return fmt.Errorf("update: stage into %s: %w", dir, err)
	}
	staged := tmp.Name()
	defer os.Remove(staged) // no-op once the rename has succeeded

	if _, err := io.Copy(tmp, res.Body); err != nil {
		tmp.Close()
		return fmt.Errorf("update: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("update: close: %w", err)
	}
	if err := os.Chmod(staged, 0o755); err != nil {
		return fmt.Errorf("update: chmod: %w", err)
	}
	if err := os.Rename(staged, exe); err != nil {
		return fmt.Errorf("update: replace %s: %w", exe, err)
	}
	return nil
}
