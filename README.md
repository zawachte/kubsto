# kubsto
query kubernetes logs and other metadata fast with duckdb and pql
## Building 

```sh
make build-linux
```

## Running

### Load up the database

If you have KUBECONFIG set:
```sh
./bin/kubsto load
```

If you don't:
```sh
./bin/kubsto --kubeconfig kubeconfig
```
### Query with pql

```sh
./bin/kubsto query "logs | where namespace == 'kube-system' | project time, log | where contains(log, 'error')"
```

### Query with duckdb

```sh
duckdb data/kubsto.db
```

## Future work

* github actions
* Investigate kubectl plugin
* kubernetes events table
* kubernetes metadata tables (pod objects)ss