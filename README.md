# background_worker.fs

LA basic implementation of a worker that uses a postgresql table as a queue

TODO:

# exponential backoff

Consider adding this

will require adding some sort of totalRetryCount so we can calculate how many retries have occurred
it will also change how we filter the tasks in getTasks ()
also may require a column that tracks next_runnable_time
  so we can filter out future dates that should not be run yet
