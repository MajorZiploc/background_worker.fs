export POSTGRES_USER='postgres'
export POSTGRES_PASSWORD='password'
export POSTGRES_PORT=5432
export POSTGRES_DB="postgres"
# naming convertion of the above vars but for the db container
export PGHOST='127.0.0.1'
export PGUSER='postgres'
export PGPASSWORD='password'
export PGPORT=5432
export PGDATABASE="postgres"
export PGDATA="/var/lib/postgresql/data/"
export PGDATALOCAL="/tmp/background_worker-fs_pgsql"

export QUEUES="main,internal,external"
export MACHINE_NAME="task_master"
export MIN_TIME_TILL_NEXT_POLL_MS=3000
export MAX_TIME_TILL_NEXT_POLL_MS=300000
export TASK_COUNT=20
