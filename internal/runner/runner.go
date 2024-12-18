package runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"

	"log/slog"

	"github.com/zawachte/kubsto/pkg/loader"
	"k8s.io/client-go/kubernetes"

	_ "github.com/marcboeker/go-duckdb"
)

type Runner interface {
	Run(context.Context) error
}

type runner struct {
	cs     kubernetes.Interface
	logger *slog.Logger
	duckdb *sql.DB
}

type RunnerParams struct {
	URI              string
	Logger           *slog.Logger
	ClientSet        kubernetes.Interface
	DatabaseLocation string
}

func NewRunner(params RunnerParams) (Runner, error) {
	databaseLocation := "data"
	if params.DatabaseLocation != "" {
		databaseLocation = params.DatabaseLocation
	}

	databaseFile := path.Join(databaseLocation, "kubsto.db")
	_, err := os.Stat(databaseFile)
	if err == nil {
		return nil, fmt.Errorf("database already exists, please delete it first")
	}

	// make the data directory if it doesn't exist
	err = os.MkdirAll(databaseLocation, 0755)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("duckdb", databaseFile)
	if err != nil {
		return nil, err
	}

	return &runner{
		logger: params.Logger,
		duckdb: db,
		cs:     params.ClientSet}, nil
}

func (r *runner) Run(ctx context.Context) error {

	loaders := []loader.Loader{}

	loaderInstance, err := loader.NewPodLogsLoader(loader.PodLogsLoaderParams{
		Logger:    r.logger,
		Duckdb:    r.duckdb,
		ClientSet: r.cs,
	})
	if err != nil {
		return err
	}

	loaders = append(loaders, loaderInstance)

	loaderInstance, err = loader.NewEventsLoader(loader.EventsLoaderParams{
		Logger:    r.logger,
		Duckdb:    r.duckdb,
		ClientSet: r.cs,
	})
	if err != nil {
		return err
	}

	loaders = append(loaders, loaderInstance)

	loaderInstance, err = loader.NewPodsLoader(loader.PodsLoaderParams{
		Logger:    r.logger,
		Duckdb:    r.duckdb,
		ClientSet: r.cs,
	})
	if err != nil {
		return err
	}

	loaders = append(loaders, loaderInstance)

	for _, loaderInstance := range loaders {
		r.logger.Info("loading data into duckdb", "loader", loaderInstance.Name())
		err := loaderInstance.Load(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
