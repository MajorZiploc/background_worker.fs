open System
open Acadian.FSharp
open Newtonsoft.Json.Linq
open Npgsql.FSharp

type Task = {
  Id: Guid
  QueueNames: string array
  Type: string
  Status: string
  Payload: JObject option
  ProgramPath: string option
  ProgramType: string option
  CreatedAt: DateTime
}

type Data() =

  member this.getConnStr() =
    Sql.host "pgsql"
    |> Sql.database "postgres"
    |> Sql.username "postgres"
    |> Sql.password "password"
    |> Sql.port 5432
    |> Sql.formatConnectionString

  member this.getTasks() =
    let x = Unchecked.defaultof<Task>
    let sql = $"
      select
        id
        , queue_names
        , type
        , status
        , payload
        , program_path
        , program_type
        , created_at
      from Task
      where status = 'QUEUED'
      limit 20
      ;
    "
    this.getConnStr ()
    |> Sql.connect
    |> Sql.query sql
    |> Sql.execute (fun read ->
        {
            Id = read.uuid "id"
            QueueNames = read.stringArray "queue_names"
            Type = read.text "type"
            Status = read.text "status"
            ProgramPath = read.textOrNone "program_path"
            ProgramType = read.textOrNone "program_type"
            Payload = read.textOrNone "payload" |> Option.map JObject.Parse
            CreatedAt = read.dateTime "created_at"
        })

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
          printfn "Processing task %A" task.Id
          do! Async.Sleep(1000)
          return 0
        })
      let! _ = work |> Async.Sequential
      do! Async.Sleep(timeTillNextPollMs)
    | _ ->
      printfn "Nothing in queue"
      do! Async.Sleep(timeTillNextPollMs)
}

let mainAsync _argv = async {
  let data = Data()
  // let! tasks = data.getTasks() |> Async.AwaitTask
  // let tasks = data.getTasks()
  // printfn "%A" tasks
  do! processQueue ()
  return 0
}

[<EntryPoint>]
let main argv = mainAsync argv |> Async.RunSynchronously
