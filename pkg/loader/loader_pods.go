package loader

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
)

type podsLoader struct {
	logger *slog.Logger
	duckdb *sql.DB
	cs     kubernetes.Interface
}

type PodsLoaderParams struct {
	Logger    *slog.Logger
	Duckdb    *sql.DB
	ClientSet kubernetes.Interface
}

func NewPodsLoader(params PodsLoaderParams) (Loader, error) {
	return &podsLoader{
		logger: params.Logger,
		duckdb: params.Duckdb,
		cs:     params.ClientSet,
	}, nil
}

func (p *podsLoader) Name() string {
	return "Pods"
}

func (p *podsLoader) Load(ctx context.Context) error {

	_, err := p.duckdb.Exec(`CREATE TABLE pods (collected_time TIMESTAMP, pod_name VARCHAR, namespace VARCHAR, pod_metadata json)`)
	if err != nil {
		return err
	}

	namespaceList, err := p.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, ns := range namespaceList.Items {
		podList, err := p.cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		for _, pod := range podList.Items {

			jsonPodBytes, err := json.Marshal(pod)
			if err != nil {
				return err
			}

			_, err = p.duckdb.Exec(`INSERT INTO pods VALUES (?, ?, ?, ?)`,
				time.Now().Format(time.RFC3339),
				pod.Name,
				ns.Name,
				string(jsonPodBytes))
			if err != nil {
				return err
			}
		}

	}

	return nil
}
