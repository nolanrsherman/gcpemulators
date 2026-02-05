package cli

import (
	"fmt"

	"github.com/nolanrsherman/gcpemulators"
	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the gcpemulators version",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), gcpemulators.Version())
			return nil
		},
	}
}
