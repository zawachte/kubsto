package runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/zawachte/kubsto/pkg/loader"
	"k8s.io/client-go/kubernetes"

	_ "github.com/marcboeker/go-duckdb"
)

type Runner interface {
	Run(context.Context) error
}

type runner struct {
	cs     kubernetes.Interface
	logger log.Logger
	duckdb *sql.DB
}

type RunnerParams struct {
	URI              string
	Logger           log.Logger
	ClientSet        kubernetes.Interface
	DatabaseLocation string
}

func NewRunner(params RunnerParams) (Runner, error) {
	databaseLocation := "kubsto.db"
	if params.DatabaseLocation != "" {
		databaseLocation = params.DatabaseLocation
	}

	_, err := os.Stat(databaseLocation)
	if err == nil {
		return nil, fmt.Errorf("database already exists, please delete it first")
	}

	db, err := sql.Open("duckdb", databaseLocation)
	if err != nil {
		return nil, err
	}

	return &runner{
		logger: params.Logger,
		duckdb: db,
		cs:     params.ClientSet}, nil
}

func (r *runner) Run(ctx context.Context) error {
	loaderInstance, err := loader.NewPodLogsLoader(loader.PodLogsLoaderParams{
		Logger:    r.logger,
		Duckdb:    r.duckdb,
		ClientSet: r.cs,
	})
	if err != nil {
		return err
	}

	return loaderInstance.Load(ctx)
}
