# GP2GP Client 
A single-client GPDB query runner using parallel cursor among GPDB cluster. 

Information about parallel cursor: https://github.com/greenplum-db/gpdb/wiki/GPDB-Parallel-Cursor

The multiple-client query runner in branch master is recommended

## Environment

- Python version: 2.7

- GPDB version: commit ab0787718f17e4c54110fe507625ede4efebfb84 at https://github.com/liming01/gpdb/commits/feature_parallelcursor


## Before running

- Install dependency on main client

```bash
$ ./env.sh
```

- Add the id_rsa.pub content of the client to the authorized_keys of both gpadmin @ master and every segments
---

## gp2gp_client.py
#### Runs a single query once, using either normal cursor or parallel cursor
```
Usage: gp2gp-client [options]

GP2GP Client Demo

Options:
  -h, --help            show this help message and exit
  -d DATABASE, --database=DATABASE
                        Connect to the database
  -H HOST, --host=HOST  Connect to the host, DEFAULT VALUE Env 'PGHOST'
  -p PORT, --port=PORT  Connect to the database on which port, DEFAULT VALUE
                        Env 'PGPORT'
  -u USER, --user=USER  username to connect the db
  -P PASSWORD, --password=PASSWORD
                        password to connect the db
  -q QUERY, --query=QUERY
                        the query which send to server
  -f FILENAME, --file=FILENAME
                        the file that stores the query
  -n, --normal          use normal cursor instead of parallel cursor
  -l LOG_LEVEL, --level=LOG_LEVEL
                        log level: info|debug
  -t, --test            not generating the result, just for performance
                        testing
```
