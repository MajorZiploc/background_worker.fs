open System
open Acadian.FSharp
open Newtonsoft.Json.Linq
open Npgsql.FSharp
open System.Threading

type Task = {
  Id: Guid
  QueueName: string
  Type: string
  Status: string
  Payload: JObject option
  ValidProgramId: Guid
  ProgramPath: string option
  ProgramType: string
  ValidProgramQueueName: string
  CreatedAt: DateTime
  ExecutedAt: DateTime option
  TimeElasped: TimeSpan option
  RetryCount: int
}

let timeTillNextPollMs = Environment.GetEnvironmentVariable "TIME_TILL_NEXT_POLL_MS" |> int

let queues = (Environment.GetEnvironmentVariable "QUEUES").Split ","
let taskCount = Environment.GetEnvironmentVariable "TASK_COUNT" |> int
let connectionString =
  Sql.host (Environment.GetEnvironmentVariable "PGHOST")
  |> Sql.database (Environment.GetEnvironmentVariable "PGDATABASE")
  |> Sql.username (Environment.GetEnvironmentVariable "PGUSER")
  |> Sql.password (Environment.GetEnvironmentVariable "PGPASSWORD")
  |> Sql.port (int (Environment.GetEnvironmentVariable "PGPORT"))
  |> Sql.formatConnectionString

type Data(connectionString: string, queues: string array, taskCount: int) =

  member this.getConnection() = connectionString |> Sql.connect

  member this.getConnectionWithDefault(connection: Sql.SqlProps option) =
    connection
    |> Option.defaultWith this.getConnection

  member this.getTasks (?connection: Sql.SqlProps) =
    let parameters = [
      ("@Queues", Sql.stringArray queues);
      ("@TaskCount", Sql.int taskCount);
    ]
    let sql = $"""
      select
        t.id
        , t.queue_name
        , t.type
        , t.status
        , t.payload
        , t.valid_program_id
        , vp.program_path
        , vp.program_type
        , vp.queue_name as valid_program_queue_name
        , t.created_at
        , t.executed_at
        , t.time_elapsed
        , t.retry_count
      from Task as t
      inner join ValidProgram as vp on t.valid_program_id = vp.id
      where
        t.status = 'QUEUED'
        and t.queue_name = any(@Queues)
      limit @TaskCount;
    """
    this.getConnectionWithDefault connection
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.execute (fun read ->
      {
        Id = read.uuid "id"
        QueueName = read.text "queue_name"
        Type = read.text "type"
        Status = read.text "status"
        ValidProgramId = read.uuid "valid_program_id"
        ProgramPath = read.textOrNone "program_path"
        ProgramType = read.text "program_type"
        ValidProgramQueueName = read.text "valid_program_queue_name"
        Payload = read.textOrNone "payload" |> Option.map JObject.Parse
        CreatedAt = read.dateTime "created_at"
        ExecutedAt = read.dateTimeOrNone "executed_at"
        TimeElasped = read.intervalOrNone "time_elapsed"
        RetryCount = read.int "retry_count"
      })

  member this.updateTask (task: Task, ?connection: Sql.SqlProps) =
    let parameters = [
      ("@Id", Sql.uuid task.Id);
      ("@ExecutedAt", Sql.timestampOrNone task.ExecutedAt);
      ("@TimeElasped", Sql.intervalOrNone task.TimeElasped);
      ("@Status", Sql.text task.Status);
      ("@RetryCount", Sql.int task.RetryCount);
    ]
    let sql = $"""
      update Task set
        executed_at = @ExecutedAt
        , time_elapsed = @TimeElasped
        , status = @Status
        , retry_count = @RetryCount
      where id = @Id;
    """
    this.getConnectionWithDefault connection
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.executeNonQuery

let executeWorkItem (connection: Sql.SqlProps) (data: Data) (task: Task) = async {
  let executedAt = DateTime.Now
  let shouldRun = task.ValidProgramQueueName = task.QueueName
  if not shouldRun then
    printfn "Program is not valid for the queue. task.QueueName: %A; task.ValidPrograQueueName: %A" task.QueueName task.ValidProgramQueueName
    let status = "FAILED"
    let n = data.updateTask { task with ExecutedAt = None; TimeElasped = None; Status = status; }, connection = connection
    return 1
  else
    printfn "Processing task: Id: %A; QueueName: %A; Type: %A" task.Id task.QueueName task.Type
    // TODO: how to handle failed tasks? will likely be different based on if we call an exe or ps1 or if the function exists in this project
    // Emulate task running
    do! Async.Sleep(1000)
    let timeElapsed = DateTime.Now - executedAt
    let retryCount = task.RetryCount - 1
    let failed =
      task.Payload
      |> Option.bind (fun jo ->
          jo.GetValue("autoFail") |> Option.ofObjForce |> Option.map (fun token -> token.Type = JTokenType.Boolean && token.Value<bool>())
      )
      |> Option.defaultValue false
    let status = if not failed then "COMPLETED" else if retryCount <= 0 then "FAILED" else "QUEUED"
    match status with
    | "FAILED" -> printfn "Task failed and is not being reattempted."
    | "QUEUED" -> printfn "Task failed and is being requeued."
    | _ -> () // No action needed for other statuses
    let n = data.updateTask { task with ExecutedAt = executedAt |> Some; TimeElasped = timeElapsed |> Some; Status = status; RetryCount = retryCount; }, connection = connection
    return if status = "FAILED" then 1 else 0
}

let processQueue (cancellationToken: CancellationToken) = async {
  let data = Data(connectionString, queues, taskCount)
  while not cancellationToken.IsCancellationRequested do
    try
      let connection = data.getConnection()
      let tasks = data.getTasks connection
      match tasks with
      | [] ->
        printfn "Nothing in queue"
        do! Async.Sleep(timeTillNextPollMs)
      | _ ->
        let beginOfProcessing = DateTime.Now
        printfn "Processing queue"
        let work = tasks |> List.map (executeWorkItem connection data)
        let! _ = work |> Async.Sequential
        let endOfProcessing = DateTime.Now
        let deltaTimeTillNextPollMs = endOfProcessing - beginOfProcessing
        do! Async.Sleep(max (timeTillNextPollMs - deltaTimeTillNextPollMs.Milliseconds) 1000)
    with
    | ex ->
      printfn "Error processing queue: %A" ex
      do! Async.Sleep(timeTillNextPollMs)
}

let mainAsync (cancellationToken: CancellationToken) = async {
  do! processQueue cancellationToken
  return 0
}

[<EntryPoint>]
let main argv =
  let cts = new CancellationTokenSource()
  Console.CancelKeyPress.Add(fun _ ->
    cts.Cancel()
    printfn "Shutting down..."
  )
  Async.RunSynchronously (mainAsync cts.Token)

