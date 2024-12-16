package querier

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/go-kit/log"

	"github.com/runreveal/pql"
	"k8s.io/client-go/kubernetes"
)

func NewQuerier(params QuerierParams) (Querier, error) {

	databaseLocation := "kubsto.db"
	if params.DatabaseLocation != "" {
		databaseLocation = params.DatabaseLocation
	}

	_, err := os.Stat(databaseLocation)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("database does not exist, please load it first")
	}

	db, err := sql.Open("duckdb", databaseLocation)
	if err != nil {
		return nil, err
	}

	return &querier{
		logger: params.Logger,
		duckdb: db}, nil
}

type Querier interface {
	Query(context.Context, string) ([]map[string]string, error)
}

type querier struct {
	logger log.Logger
	duckdb *sql.DB
}

type QuerierParams struct {
	URI              string
	Logger           log.Logger
	ClientSet        kubernetes.Interface
	DatabaseLocation string
}

func (q *querier) Query(ctx context.Context, queryString string) ([]map[string]string, error) {
	query, err := pql.Compile(queryString)
	if err != nil {
		return nil, err
	}

	q.logger.Log("compiled query", "query", query)

	// Execute the query
	rows, err := q.duckdb.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create a slice of interface{}'s to hold each column value
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	results := []map[string]string{}

	// Iterate over the rows
	for rows.Next() {
		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		rowMap := map[string]string{}

		// Print the raw values of the row
		for i, col := range columns {
			val := values[i]

			// Convert the value to a string
			var v string
			switch val := val.(type) {
			case nil:
				v = "NULL"
			case []byte:
				v = string(val)
			default:
				v = fmt.Sprintf("%v", val)
			}
			rowMap[col] = v
		}
		results = append(results, rowMap)
	}

	return results, nil
}
