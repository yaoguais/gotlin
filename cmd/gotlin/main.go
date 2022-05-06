package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/urfave/cli/v2"
)

var (
	output  = fmt.Print
	outputf = fmt.Printf
)

func main() {
	err := getApp().Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getApp() *cli.App {
	app := &cli.App{
		Name:  "gotlin",
		Usage: "a DAG-based distributed task engine for massively parallel computing",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "Load configuration from `FILE`",
			},
			&cli.BoolFlag{
				Name:    "compute-node",
				Aliases: []string{"p"},
				Usage:   "As the role of the compute node, false starts as a service node",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Start a service node or compute node",
				Action: func(c *cli.Context) error {
					config := c.String("config")
					isComputeNode := c.Bool("compute-node")

					outputf("config %s isComputeNode %v\n", config, isComputeNode)
					return nil
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	return app
}
