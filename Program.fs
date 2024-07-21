open System
open Acadian.FSharp
open Newtonsoft.Json.Linq
open Npgsql.FSharp

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

let queues = [| "main"; "internal"; "external" |]

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
        ("@Queues", Sql.stringArray queues)
    ]
    let sql = $"
      select
        id
        , queue_name
        , type
        , status
        , payload
        , program_path
        , program_type
        , created_at
        , executed_at
        , time_elapsed
      from Task
      where
        status = 'QUEUED'
        and queue_name = Any(@Queues)
      limit 20
      ;
    "
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
        ("@Id", Sql.uuid task.Id)
        ("@ExecutedAt", Sql.timestampOrNone task.ExecutedAt)
        ("@TimeElasped", Sql.intervalOrNone task.TimeElasped)
        ("@Status", Sql.text task.Status)
    ]
    let sql = $"
      UPDATE Task
        SET
          executed_at = @ExecutedAt
          , time_elapsed = @TimeElasped
          , status = @Status
      WHERE id = @Id
      ;
    "
    this.getConnStr ()
    |> Sql.connect
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.executeNonQuery

let timeTillNextPollMs = 3000

// Function to process actions from the queue asynchronously
let rec processQueue () = async {
  let data = Data()
  while true do
    let tasks = data.getTasks()
    match tasks |> List.length with
    | i when i > 0 ->
      printfn "Process queue"
      let work = tasks |> List.map (fun task -> async {
          let executedAt = DateTime.Now
          printfn "Processing task %A" task.Id
          // TODO: how to handle failed tasks? will likely be different based on if we call an exe or ps1 or if the function exists in this project
          // emulate task running
          do! Async.Sleep(1000)
          let timeElapsed = DateTime.Now - executedAt
          let status = "COMPLETED"
          let n = data.updateTask ({task with ExecutedAt = executedAt |> Option.Some; TimeElasped = timeElapsed |> Option.Some; Status = status})
          return 0
        })
      let! _ = work |> Async.Sequential
      do! Async.Sleep(timeTillNextPollMs)
    | _ ->
      printfn "Nothing in queue"
      do! Async.Sleep(timeTillNextPollMs)
}

let mainAsync _argv = async {
  // let data = Data()
  // let tasks = data.getTasks()
  // printfn "%A" tasks
  do! processQueue ()
  return 0
}

[<EntryPoint>]
let main argv = mainAsync argv |> Async.RunSynchronously
