package main

import (
	"encoding/json"
	"fmt"
	"os"

	"context"

	"log/slog"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/urfave/cli/v3"
	"github.com/zawachte/kubsto/internal/querier"
	"github.com/zawachte/kubsto/internal/runner"
	"github.com/zawachte/kubsto/pkg/kubeclient"
)

func main() {
	kubeconfig := ""
	databaseLocation := ""
	outputFormat := ""
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
						Name:        "data-dir",
						Value:       "data",
						Usage:       "database location",
						Destination: &databaseLocation,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

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
						Name:        "data-dir",
						Value:       "data",
						Usage:       "database location",
						Destination: &databaseLocation,
						Aliases:     []string{"d"},
					},
					&cli.StringFlag{
						Name:        "output-format",
						Value:       "text",
						Usage:       "output format",
						Destination: &outputFormat,
						Aliases:     []string{"o"},
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

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

					return printResults(results, outputFormat)
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}

}

func printResults(results []map[string]string, outputFormat string) error {
	switch outputFormat {
	case "json":
		// marshal results to json
		marshaledResults, err := json.Marshal(results)
		if err != nil {
			return err
		}

		fmt.Println(string(marshaledResults))
		return nil
	case "text":
		// print results as text
		for _, result := range results {
			for key, value := range result {
				fmt.Printf("%s: %s\n", key, value)
			}
			fmt.Println("----------")
		}
		return nil
	default:
		return fmt.Errorf("unknown output format: %s", outputFormat)
	}
}
