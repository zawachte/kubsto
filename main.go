package main

import (
	"encoding/json"
	"fmt"
	"os"

	"context"

	"github.com/go-kit/log"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/urfave/cli/v3"
	"github.com/zawachte/kubsto/internal/querier"
	"github.com/zawachte/kubsto/internal/runner"
	"github.com/zawachte/kubsto/pkg/kubeclient"
)

func main() {
	kubeconfig := ""
	databaseLocation := ""
	cmd := &cli.Command{
		Commands: []*cli.Command{
			{
				Name:    "load",
				Aliases: []string{"l"},
				Usage:   "load metadata from kubernetes cluster",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "kubeconfig",
						Value:       "",
						Usage:       "kubeconfig file",
						Destination: &kubeconfig,
					},
					&cli.StringFlag{
						Name:        "database-location",
						Value:       "kubsto.db",
						Usage:       "database location",
						Destination: &databaseLocation,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

					cs, err := kubeclient.CreateClientSet(kubeconfig)
					if err != nil {
						return err
					}
					rr, err := runner.NewRunner(runner.RunnerParams{
						Logger:           logger,
						ClientSet:        cs,
						DatabaseLocation: databaseLocation,
					})
					if err != nil {
						return err
					}

					err = rr.Run(context.Background())
					if err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:    "query",
				Aliases: []string{"q"},
				Usage:   "query duckdb database",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "database-location",
						Value:       "kubsto.db",
						Usage:       "database location",
						Destination: &databaseLocation,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
					if cmd.NArg() == 0 {
						return fmt.Errorf("no query string provided")
					}
					queryString := cmd.Args().Get(0)
					querierInstance, err := querier.NewQuerier(querier.QuerierParams{
						Logger:           logger,
						DatabaseLocation: databaseLocation,
					})
					if err != nil {
						return err
					}

					results, err := querierInstance.Query(context.Background(), queryString)
					if err != nil {
						return err
					}

					// marshal results to json
					marshaledResults, err := json.Marshal(results)
					if err != nil {
						return err
					}

					fmt.Println(string(marshaledResults))
					return nil
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}

}
