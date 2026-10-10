package cli

import (
	"runtime"

	"github.com/spf13/cobra"
)

// newVersionCmd builds `mandatum version`.
//
// It reports the commit and build date alongside the tag because a version
// string alone cannot distinguish a release from a local build of the same
// tag, and the first question about a misbehaving binary is always which one it
// actually is.
func newVersionCmd(v Version) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			if u.format == FormatJSON {
				return u.json(map[string]string{
					"version": v.Version,
					"commit":  v.Commit,
					"date":    v.Date,
					"go":      runtime.Version(),
					"os":      runtime.GOOS,
					"arch":    runtime.GOARCH,
				})
			}
			u.printf("mandatum %s\n", v.Version)
			if v.Commit != "" {
				u.printf("  commit %s\n", v.Commit)
			}
			if v.Date != "" {
				u.printf("  built  %s\n", v.Date)
			}
			u.printf("  go     %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
			return nil
		},
	}
}
