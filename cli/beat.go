package cli

import (
	"github.com/goodking-bq/go-celery"
	"github.com/spf13/cobra"
)

func NewBeatCommand(app *celery.Celery) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "beat",
		Short: "run gocelery beat",
		Long:  "run gocelery beat",
		Run: func(cmd *cobra.Command, args []string) {
			app.StartBeat()
		},
	}
	return cmd
}
