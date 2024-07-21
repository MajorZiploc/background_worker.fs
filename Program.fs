open System
open Acadian.FSharp
open Newtonsoft.Json.Linq
open Npgsql.FSharp
open System.Threading
open System.Threading.Tasks

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

// TODO: use .environment variables or a json config for these
let queues = [| "main"; "internal"; "external" |]
let timeTillNextPollMs = 3000
let taskCount = 20

type Data() =
  member this.getConnStr() =
    Sql.host "pgsql"
    |> Sql.database "postgres"
    |> Sql.username "postgres"
    |> Sql.password "password"
    |> Sql.port 5432
    |> Sql.formatConnectionString

  member this.getTasks() =
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
    this.getConnStr ()
    |> Sql.connect
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

  member this.updateTask(task: Task) =
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
    this.getConnStr ()
    |> Sql.connect
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.executeNonQuery

let rec processQueue (cancellationToken: CancellationToken) = async {
  let data = Data()
  while not cancellationToken.IsCancellationRequested do
    try
      let tasks = data.getTasks()
      match tasks with
      | [] ->
        printfn "Nothing in queue"
        do! Async.Sleep(timeTillNextPollMs)
      | _ ->
        let beginOfProcessing = DateTime.Now
        printfn "Processing queue"
        let work = tasks |> List.map (fun task -> async {
          let executedAt = DateTime.Now
          printfn "Processing task %A" task.Id
          // TODO: how to handle failed tasks? will likely be different based on if we call an exe or ps1 or if the function exists in this project
          // Emulate task running
          do! Async.Sleep(1000)
          let timeElapsed = DateTime.Now - executedAt
          let status = "COMPLETED"
          let n = data.updateTask ({ task with ExecutedAt = executedAt |> Some ; TimeElasped = timeElapsed |> Some ; Status = status })
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

