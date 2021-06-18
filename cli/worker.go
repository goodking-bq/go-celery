package cli

import (
	"github.com/spf13/cobra"
	"gitlab.kube.2xi.com/yunwei/platform-backend-tasks/celery"
	"runtime"
)

func NewWorkerCommand(app *celery.Celery, config *celery.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "run gocelery worker",
		Long:  "run gocelery worker",
		Run: func(cmd *cobra.Command, args []string) {
			app.StartWorkerForever()
		},
	}
	cmd.Flags().IntVarP(&config.Concurrency, "concurrency", "c", runtime.NumCPU(),
		"Number of child processes processing the queue. The default is the number of CPUs available on your system.")
	cmd.Flags().StringArrayVarP(&config.Queues, "queues",
		"Q", []string{"celery"},
		"List of queues to enable for this worker, separated by comma. By default all configured queues are enabled.\nExample: -Q video -Q image")
	return cmd
}
