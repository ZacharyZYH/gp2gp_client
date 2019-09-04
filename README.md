# GP2GP Client 
A mutiple-client GPDB query runner using parallel cursor among GPDB cluster. 

Information about parallel cursor: https://github.com/greenplum-db/gpdb/wiki/GPDB-Parallel-Cursor

If you want single-client query runner, see branch 1.X

## Environment

- Python version: 2.7

- GPDB version: commit ab0787718f17e4c54110fe507625ede4efebfb84 at https://github.com/liming01/gpdb/commits/feature_parallelcursor


## Before running

- Install dependency on main client

```bash
$ ./env.sh
```

- Setup clients config file(default name: clients.conf)
```
10.1.1.1
10.1.1.2
10.1.1.3
```

- Add the id_rsa.pub content of the main client to the authorized_keys of both root and gpadmin @ master, every segments, and every client host

- Add the gpadmin's id_rsa.pub on each client host to the authorized_keys of gpadmin@ every segment machine. (can be done with gpssh-exkeys tool)

- Deploy the retrieve clients to client hosts

Run the gp2gp_client.py with -D, -H, -p, -u, -P, -c  options

Example:
```bash
$ python gp2gp_client.py -D -H localhost -p 15432 -u gpadmin -c client.conf
```

---

## gp2gp_client.py
Runs a single query once, using either normal cursor or parallel cursor

## gp2gp_perf_test.py
Iterate through 22 TPC-H queries. For each query, run it in both type of cursor. Record the execution time and output to result.csv.