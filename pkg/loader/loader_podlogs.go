package loader

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"io"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
)

type podLogsLoader struct {
	logger *slog.Logger
	duckdb *sql.DB
	cs     kubernetes.Interface
}

type PodLogsLoaderParams struct {
	Logger    *slog.Logger
	Duckdb    *sql.DB
	ClientSet kubernetes.Interface
}

func NewPodLogsLoader(params PodLogsLoaderParams) (Loader, error) {
	return &podLogsLoader{
		logger: params.Logger,
		duckdb: params.Duckdb,
		cs:     params.ClientSet,
	}, nil
}

func (p *podLogsLoader) Name() string {
	return "PodLogs"
}

func (p *podLogsLoader) Load(ctx context.Context) error {

	_, err := p.duckdb.Exec(`CREATE TABLE logs (time TIMESTAMP, log VARCHAR, pod_name VARCHAR, container_name VARCHAR, namespace VARCHAR)`)
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
			for _, container := range pod.Spec.Containers {
				req := p.cs.CoreV1().Pods(ns.Name).GetLogs(pod.Name, &corev1.PodLogOptions{
					Timestamps: true,
					Container:  container.Name,
				})

				podLogs, err := req.Stream(ctx)
				if err != nil {
					p.logger.Info("error streaming pod logs", "pod_name", pod.Name, "container_name", container.Name, "error", err)
					continue
				}
				defer podLogs.Close()

				buf := new(bytes.Buffer)

				_, err = io.Copy(buf, podLogs)
				if err != nil {
					return err
				}

				err = p.loadLogsToDuckDB(buf, pod.Name, container.Name, ns.Name)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (p *podLogsLoader) loadLogsToDuckDB(rawLogs *bytes.Buffer, podName, containerName, namespace string) error {
	scanner := bufio.NewScanner(rawLogs)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		logLineSplit := strings.Split(scanner.Text(), " ")
		tim, err := time.Parse(time.RFC3339, logLineSplit[0])
		if err != nil {
			p.logger.Info("Skipping log due to invalid parse", "Error", err.Error())
			continue
		}
		_, err = p.duckdb.Exec(`INSERT INTO logs VALUES (?, ?, ?, ?, ?)`, tim, scanner.Text(), podName, containerName, namespace)
		if err != nil {
			p.logger.Info("Skipping log due to invalid parse", "Error", err.Error())
			continue
		}
	}

	return nil
}
