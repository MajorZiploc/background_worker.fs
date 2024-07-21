open Acadian.FSharp
open Acadian.Dapper.Fs
open FSharp.Control.Tasks.V2.ContextInsensitive
open Dapper
open Npgsql
open Npgsql.FSharp

type Task = {
  // id: string array
  // QueueNames: string array
  Type: string
  Status: string
  // Payload: string
  // Result: string
  ProgramPath: string option
  ProgramType: string option
  // CreatedAt: string
  // ExecutedAt: string
  // TimeElapsed: string
}

let connectionString = "Host=127.0.0.1:5432;Database=postgres;Username=postgres;Password=postgres"

type Data() =
  // member this.getConn() = safeSqlConnection connectionString
  member this.getConn() = new NpgsqlConnection(connectionString)

  member this.getConnStr() =
    Sql.host "pgsql"
    |> Sql.database "postgres"
    |> Sql.username "postgres"
    |> Sql.password "password"
    |> Sql.port 5432
    |> Sql.formatConnectionString

  member this.getTasks() = task {
    let x = Unchecked.defaultof<Task>
    let sql = $"
      select
        --  = Id
        -- queue_names
        {nameof x.Type} = type
        ,status
        -- ,payload
        -- ,result
        ,{nameof x.ProgramPath} = program_path
        ,{nameof x.ProgramType} = program_type
        -- , = created_at
        -- , = executed_at
        -- , = time_elapsed
      from \"Task\"
      ;
    "
    use conn = this.getConn()
    let! tasks = conn.QueryAsync<Task>(sql) |> Task.map (Option.ofObjForce)
    return tasks |> Option.map Seq.toList
  }

  member this.getTasks1() =
    let x = Unchecked.defaultof<Task>
    let sql = $"
      select
        --  = Id
        -- queue_names
        {nameof x.Type} = type
        ,{nameof x.Status} = status
        -- ,payload
        -- ,result
        ,{nameof x.ProgramPath} = program_path
        ,{nameof x.ProgramType} = program_type
        -- , = created_at
        -- , = executed_at
        -- , = time_elapsed
      from \"Task\"
      ;
    "
    this.getConnStr ()
    |> Sql.connect
    |> Sql.query sql
    |> Sql.execute (fun read ->
        {
            Type = read.text "Type"
            Status = read.text "Status"
            ProgramPath = read.textOrNone "ProgramPath"
            ProgramType = read.textOrNone "ProgramType"
            // FirstName = read.int "first_name"
            // LastName = read.textOrNone "last_name" // reading nullable column
        })

  // member this.getTasks() = task {
  //   let x = Unchecked.defaultof<Task>
  //   let sql = $"
  //     select
  //       --  = Id
  //       -- queue_names
  //       {nameof x.Type} = type
  //       ,status
  //       -- ,payload
  //       -- ,result
  //       ,{nameof x.ProgramPath} = program_path
  //       ,{nameof x.ProgramType} = program_type
  //       -- , = created_at
  //       -- , = executed_at
  //       -- , = time_elapsed
  //     from Task
  //     ;
  //   "
  //   use conn = this.getConn()
  //   let! items = conn.QueryAsync<Task>(sql) |> Task.map (Option.ofObjForce)
  //   return items |> Option.map Seq.toList
  // }

let timeTillNextPollMs = 3000

let checkDb () = []

// Function to process actions from the queue asynchronously
let rec processQueue () = async {
  while true do
    match checkDb () with
    | f::r ->
      printfn "Process queue"
      do! Async.Sleep(1000)
    | [] ->
      printfn "Nothing in queue"
      do! Async.Sleep(timeTillNextPollMs)
}

let mainAsync _argv = async {
  let data = Data()
  // let! tasks = data.getTasks1() |> Async.AwaitTask
  let tasks = data.getTasks1()
  printfn "%A" tasks
  // do! processQueue ()
  return 0
}

[<EntryPoint>]
let main argv = mainAsync argv |> Async.RunSynchronously
