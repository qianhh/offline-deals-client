package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/filedrive-team/offline-deals-client/settings"
	"github.com/filedrive-team/offline-deals-client/syncer"
	"github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var logging = log.Logger("main")

func main() {
	local := []*cli.Command{
		DaemonCmd,
		InitCmd,
	}

	app := &cli.App{
		Name:        "offline-deals-client",
		Commands:    local,
		Description: "filedrive datasets offline deals client",
		Flags:       []cli.Flag{},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n\n", err)
		os.Exit(1)
	}
}

var DaemonCmd = &cli.Command{
	Name:  "daemon",
	Usage: "Start an offline-deals-client daemon process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "config",
			Value: "conf/app.toml",
			Usage: "set config file path",
		},
		&cli.StringFlag{
			Name:  "loglevel",
			Value: "debug",
			Usage: "set log level",
		},
		&cli.StringFlag{
			Name:    "datadir",
			Usage:   "directory to store data in",
			Value:   "./data",
			EnvVars: []string{"ODC_DATADIR"},
		},
	},
	Action: func(c *cli.Context) error {
		configFile := c.String("config")
		loglevel := c.String("loglevel")

		log.SetLogLevel("main", loglevel)
		log.SetLogLevel("syncer", loglevel)

		settings.Setup(configFile)

		ddir := c.String("datadir")
		err := os.MkdirAll(ddir, 0777)
		if err != nil {
			return err
		}

		carDir := filepath.Join(ddir, "car")
		err = os.MkdirAll(carDir, 0777)
		if err != nil {
			return err
		}

		syncer := syncer.NewSyncer(filepath.Join(ddir, "deals"), carDir)
		ctx, cancel := context.WithCancel(context.Background())
		exitCh := make(chan struct{})
		go func() {
			syncer.Run(ctx)
			close(exitCh)
		}()

		// Wait for interrupt signal to gracefully shutdown the server with
		// a timeout of 5 seconds.
		quit := make(chan os.Signal, 1)
		// kill (no param) default send syscall.SIGTERM
		// kill -2 is syscall.SIGINT
		// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		logging.Info("Shutdown Server ...")

		cancel()
		<-exitCh

		logging.Info("Server exit")
		return nil
	},
}

var InitCmd = &cli.Command{
	Name:  "init",
	Usage: "Initialize offline-deals-client",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "config",
			Value: "conf/app.toml",
			Usage: "set output config file path",
		},
	},
	Action: func(c *cli.Context) error {
		configFile := c.String("config")

		cfgDir := filepath.Dir(configFile)
		err := os.MkdirAll(cfgDir, 0777)
		if err != nil {
			return err
		}

		config := settings.GetDefaultConfig()
		var buffer bytes.Buffer
		if err := toml.NewEncoder(&buffer).Encode(config); err != nil {
			return err
		}
		return ioutil.WriteFile(configFile, buffer.Bytes(), 0777)
	},
}
