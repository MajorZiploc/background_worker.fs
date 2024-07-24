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
  ProgramPath: string option
  ProgramType: string option
  CreatedAt: DateTime
  ExecutedAt: DateTime option
  TimeElasped: TimeSpan option
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
        id, queue_name, type, status, payload, program_path, program_type,
        created_at, executed_at, time_elapsed
      from Task
      where
        status = 'QUEUED'
        and queue_name = any(@Queues)
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
        ProgramPath = read.textOrNone "program_path"
        ProgramType = read.textOrNone "program_type"
        Payload = read.textOrNone "payload" |> Option.map JObject.Parse
        CreatedAt = read.dateTime "created_at"
        ExecutedAt = read.dateTimeOrNone "executed_at"
        TimeElasped = read.intervalOrNone "time_elapsed"
      })

  member this.updateTask (task: Task, ?connection: Sql.SqlProps) =
    let parameters = [
      ("@Id", Sql.uuid task.Id);
      ("@ExecutedAt", Sql.timestampOrNone task.ExecutedAt);
      ("@TimeElasped", Sql.intervalOrNone task.TimeElasped);
      ("@Status", Sql.text task.Status);
    ]
    let sql = $"""
      update Task
      set executed_at = @ExecutedAt, time_elapsed = @TimeElasped, status = @Status
      where id = @Id;
    """
    this.getConnectionWithDefault connection
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.executeNonQuery

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
        let work = tasks |> List.map (fun task -> async {
          let executedAt = DateTime.Now
          printfn "Processing task: Id: %A; QueueName: %A; Type: %A" task.Id task.QueueName task.Type
          // TODO: how to handle failed tasks? will likely be different based on if we call an exe or ps1 or if the function exists in this project
          // Emulate task running
          do! Async.Sleep(1000)
          let timeElapsed = DateTime.Now - executedAt
          let status = "COMPLETED"
          let n = data.updateTask ({ task with ExecutedAt = executedAt |> Some ; TimeElasped = timeElapsed |> Some ; Status = status }), connection = connection
          return 0
        })
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

