package runner

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	_, err := r.duckdb.Exec(`CREATE TABLE logs (time TIMESTAMP, log VARCHAR, pod_name VARCHAR, container_name VARCHAR, namespace VARCHAR)`)
	if err != nil {
		return err
	}

	namespaceList, err := r.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, ns := range namespaceList.Items {
		podList, err := r.cs.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		for _, pod := range podList.Items {
			for _, container := range pod.Spec.Containers {
				req := r.cs.CoreV1().Pods(ns.Name).GetLogs(pod.Name, &corev1.PodLogOptions{
					Timestamps: true,
					Container:  container.Name,
				})

				podLogs, err := req.Stream(ctx)
				if err != nil {
					r.logger.Log("error streaming pod logs", "pod_name", pod.Name, "container_name", container.Name, "error", err)
					continue
				}
				defer podLogs.Close()

				buf := new(bytes.Buffer)

				_, err = io.Copy(buf, podLogs)
				if err != nil {
					return err
				}

				err = r.loadLogsToDuckDB(buf, pod.Name, container.Name, ns.Name)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *runner) loadLogsToDuckDB(rawLogs *bytes.Buffer, podName, containerName, namespace string) error {
	scanner := bufio.NewScanner(rawLogs)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		logLineSplit := strings.Split(scanner.Text(), " ")
		tim, err := time.Parse(time.RFC3339, logLineSplit[0])
		if err != nil {
			r.logger.Log("Skipping log due to invalid parse", "Error", err.Error())
			continue
		}
		_, err = r.duckdb.Exec(`INSERT INTO logs VALUES (?, ?, ?, ?, ?)`, tim, scanner.Text(), podName, containerName, namespace)
		if err != nil {
			r.logger.Log("Skipping log due to invalid parse", "Error", err.Error())
			continue
		}
	}

	return nil
}
