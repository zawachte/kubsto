package loader

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"time"

	"k8s.io/client-go/kubernetes"
)

type eventsLoader struct {
	logger *slog.Logger
	duckdb *sql.DB
	cs     kubernetes.Interface
}

type EventsLoaderParams struct {
	Logger    *slog.Logger
	Duckdb    *sql.DB
	ClientSet kubernetes.Interface
}

func NewEventsLoader(params EventsLoaderParams) (Loader, error) {
	return &eventsLoader{
		logger: params.Logger,
		duckdb: params.Duckdb,
		cs:     params.ClientSet,
	}, nil
}

func (p *eventsLoader) Name() string {
	return "Events"
}

func (p *eventsLoader) Load(ctx context.Context) error {

	_, err := p.duckdb.Exec(`CREATE TABLE events (time TIMESTAMP, event json, namespace VARCHAR)`)
	if err != nil {
		return err
	}

	namespaceList, err := p.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, ns := range namespaceList.Items {
		eventList, err := p.cs.CoreV1().Events(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		for _, event := range eventList.Items {

			jsonEventBytes, err := json.Marshal(event)
			if err != nil {
				return err
			}

			_, err = p.duckdb.Exec(`INSERT INTO events VALUES (?, ?, ?)`,
				event.EventTime.Time.Format(time.RFC3339),
				string(jsonEventBytes), ns.Name)
			if err != nil {
				return err
			}
		}

	}

	return nil
}
