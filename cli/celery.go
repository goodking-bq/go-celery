package cli

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gitlab.kube.2xi.com/yunwei/platform-backend-tasks/celery"
	"os"
)

var (
	cfgFile   string
	envPrefix = "CELERY"
)

type CeleryCli struct {
	Conf          *celery.Config
	App           *celery.Celery
	RootCommand   *cobra.Command
	WorkerCommand *cobra.Command
	BeatCommand   *cobra.Command
}

func (cli *CeleryCli) Run() error {
	return cli.RootCommand.Execute()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".celery" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".celery")
	}
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
func bindFlags(cmd *cobra.Command, v *viper.Viper) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		// Apply the viper config value to the flag when the flag is not set and viper has a value
		if !f.Changed && v.IsSet(f.Name) {
			val := v.Get(f.Name)
			cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
		}
	})
}
func NewCeleryCli() *CeleryCli {
	root := &cobra.Command{
		Use:   "go celery app ",
		Short: "run go celery app",
		Run: func(cmd *cobra.Command, args []string) {
			println(celery.C.BrokerUrl)
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			initConfig()
			bindFlags(cmd, viper.GetViper())
		},
	}
	root.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.celery.json)")
	root.PersistentFlags().StringVarP(&celery.C.BrokerUrl, "broker", "b", "redis://", "broker url")
	root.PersistentFlags().StringVar(&celery.C.BackendUrl, "result-backend", "redis://", "result backend  url")
	return &CeleryCli{RootCommand: root}
}
