open System
open System.Text
open System.Diagnostics
open Acadian.FSharp
open Newtonsoft.Json.Linq
open Npgsql.FSharp
open System.Threading

type TaskEntry = {
  Id: Guid
  MachineName: string
  QueueName: string
  Type: string
  Status: string
  ExitCode: int option
  Payload: JObject option
  ValidProgramId: Guid
  ProgramPath: string option
  ProgramCommand: string option
  ValidProgramMachineName: string option
  CreatedAt: DateTime
  RunnableAt: DateTime
  ExecutedAt: DateTime option
  TimeElapsed: TimeSpan option
  AttemptCount: int
  RetryCount: int
}

let minTimeTillNextPollMs = Environment.GetEnvironmentVariable "MIN_TIME_TILL_NEXT_POLL_MS" |> int
let maxTimeTillNextPollMs = Environment.GetEnvironmentVariable "MAX_TIME_TILL_NEXT_POLL_MS" |> int

let queues = (Environment.GetEnvironmentVariable "QUEUES").Split ","
let machineName = Environment.GetEnvironmentVariable "MACHINE_NAME"
let taskCount = Environment.GetEnvironmentVariable "TASK_COUNT" |> int
let connectionString =
  Sql.host (Environment.GetEnvironmentVariable "PGHOST")
  |> Sql.database (Environment.GetEnvironmentVariable "PGDATABASE")
  |> Sql.username (Environment.GetEnvironmentVariable "PGUSER")
  |> Sql.password (Environment.GetEnvironmentVariable "PGPASSWORD")
  |> Sql.port (int (Environment.GetEnvironmentVariable "PGPORT"))
  |> Sql.formatConnectionString

type Data(connectionString: string, queues: string array, taskCount: int, machineName: string) =

  member this.getConnection() = connectionString |> Sql.connect

  member this.getConnectionWithDefault(connection: Sql.SqlProps option) =
    connection
    |> Option.defaultWith this.getConnection

  member this.getTasks (?connection: Sql.SqlProps) =
    let parameters = [
      ("@Queues", Sql.stringArray queues);
      ("@MachineName", Sql.string machineName);
      ("@TaskCount", Sql.int taskCount);
    ]
    let sql = $"""
      select
        t.id
        , t.machine_name
        , t.queue_name
        , t.Type
        , t.status
        , t.exit_code
        , t.payload
        , t.valid_program_id
        , vp.program_path
        , vp.program_command
        , vp.machine_name as valid_program_machine_name
        , t.created_at
        , t.runnable_at
        , t.executed_at
        , t.time_elapsed
        , t.attempt_count
        , t.retry_count
      from TaskEntry as t
      left join ValidProgram as vp on t.valid_program_id = vp.id
      where
        t.status = 'QUEUED'
        and t.queue_name = any(@Queues)
        and t.machine_name = @MachineName
        and t.runnable_at < NOW()
      limit @TaskCount;
    """
    this.getConnectionWithDefault connection
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.execute (fun read ->
      {
        Id = read.uuid "id"
        MachineName = read.text "machine_name"
        QueueName = read.text "queue_name"
        Type = read.text "type"
        Status = read.text "status"
        ExitCode = read.intOrNone "exit_code"
        ValidProgramId = read.uuid "valid_program_id"
        ProgramPath = read.textOrNone "program_path"
        ProgramCommand = read.textOrNone "program_command"
        ValidProgramMachineName = read.textOrNone "valid_program_machine_name"
        Payload = read.textOrNone "payload" |> Option.map JObject.Parse
        CreatedAt = read.dateTime "created_at"
        RunnableAt = read.dateTime "runnable_at"
        ExecutedAt = read.dateTimeOrNone "executed_at"
        TimeElapsed = read.intervalOrNone "time_elapsed"
        AttemptCount = read.int "attempt_count"
        RetryCount = read.int "retry_count"
      })

  member this.updateTask (taskEntry: TaskEntry, ?connection: Sql.SqlProps) =
    let parameters = [
      ("@Id", Sql.uuid taskEntry.Id);
      ("@ExecutedAt", Sql.timestamptzOrNone taskEntry.ExecutedAt);
      ("@TimeElapsed", Sql.intervalOrNone taskEntry.TimeElapsed);
      ("@Status", Sql.text taskEntry.Status);
      ("@AttemptCount", Sql.int taskEntry.AttemptCount);
      ("@RunnableAt", Sql.timestamptz taskEntry.RunnableAt);
      ("@ExitCode", Sql.intOrNone taskEntry.ExitCode);
    ]
    let sql = $"""
      update TaskEntry set
        executed_at = @ExecutedAt
        , time_elapsed = @TimeElapsed
        , status = @Status
        , attempt_count = @AttemptCount
        , runnable_at = @RunnableAt
        , exit_code = @ExitCode
      where id = @Id;
    """
    this.getConnectionWithDefault connection
    |> Sql.query sql
    |> Sql.parameters parameters
    |> Sql.executeNonQuery

let getNextRunnableDate (taskEntry: TaskEntry) =
  let baseDelay = TimeSpan.FromSeconds(5.0)
  let delay = baseDelay * Math.Pow(2.0, float taskEntry.AttemptCount)
  let maxDelay = TimeSpan.FromMinutes(10.0)
  let actualDelay = min delay.TotalSeconds maxDelay.TotalSeconds |> TimeSpan.FromSeconds
  DateTime.UtcNow.Add(actualDelay)

let stringToBase64 (input: string) =
  let bytes = Encoding.UTF8.GetBytes(input)
  Convert.ToBase64String(bytes)

let parseTaskArgs (arguments: JObject option) =
  arguments
  |> Option.bind (fun args ->
    match args.TryGetValue("args") with
    | true, (:? JArray as m) ->
      let stringArgs =
        m |> Seq.map (function
          | :? JValue as v -> v.ToString() // Handle int, string, etc.
          | obj -> sprintf "%A" (obj.ToString() |> stringToBase64) // Handle JObject or other types
        )
        |> String.concat " "
      Some stringArgs
    | _ -> None
  )

let processTask (command: string) (workingDir: string) (arguments: JObject option) =
  let processor = new Process()
  processor.StartInfo.FileName <- command
  let args = parseTaskArgs arguments
  match args with
  | Some a -> processor.StartInfo.Arguments <- a
  | None -> ()
  processor.StartInfo.WorkingDirectory <- workingDir
  processor.StartInfo.RedirectStandardOutput <- true
  processor.StartInfo.UseShellExecute <- false
  // TODO: look into if this boolean is relative for the background_worker
  processor.Start() |> ignore
  // let output = processor.StandardOutput.ReadToEnd()
  // printfn "%A" output
  processor.WaitForExit()
  let exitCode = processor.ExitCode
  // output, exitCode
  exitCode

let executeWorkItem (connection: Sql.SqlProps) (data: Data) (taskEntry: TaskEntry) = async {
  let mutable taskEntryMut = taskEntry
  let executedAt = DateTime.UtcNow
  let shouldRun =
    (taskEntryMut.ValidProgramMachineName |> Option.map ((=) taskEntryMut.MachineName) |? false)
    && (taskEntryMut.ProgramCommand |> Option.isSome)
  if not shouldRun then
    printfn "Program is not valid for the machine. MachineName: %A; ValidProgramMachineName: %A" taskEntryMut.MachineName taskEntryMut.ValidProgramMachineName
    let status = "FAILED"
    let n = data.updateTask ({ taskEntryMut with ExecutedAt = None; TimeElapsed = None; Status = status; }, connection)
    return Some 1
  else
    printfn "Processing TaskEntry: Id: %A; QueueName: %A; ProgramCommand: %A;" taskEntryMut.Id taskEntryMut.QueueName taskEntryMut.ProgramCommand
    let exitCodeResult = tryResult (fun () -> processTask (taskEntryMut.ProgramCommand |? "") (taskEntryMut.ProgramPath |? "") taskEntryMut.Payload)
    let timeElapsed = DateTime.UtcNow - executedAt
    let exitCodeStatus = exitCodeResult |> Result.map (fun exitCode -> Some exitCode, if exitCode = 0 then "COMPLETED" else "FAILED") |> Result.mapError (fun exn ->
      printfn "%A" exn
      None, "MALFORMED_TASK"
    )
    let statusCodeMeta =
      match exitCodeStatus with
      | Ok v -> v
      | Error v -> v
    taskEntryMut <- { taskEntryMut with ExecutedAt = executedAt |> Some; TimeElapsed = timeElapsed |> Some; Status = statusCodeMeta |> snd; ExitCode = statusCodeMeta |> fst }
    match statusCodeMeta with
    | exitCode, "FAILED" when taskEntryMut.RetryCount > 0 && taskEntryMut.AttemptCount < taskEntryMut.RetryCount ->
      let attemptCount = taskEntryMut.AttemptCount + 1
      let runnableAt =  taskEntryMut |> getNextRunnableDate
      taskEntryMut <- { taskEntryMut with AttemptCount = attemptCount; RunnableAt = runnableAt; Status = "QUEUED" }
      printfn "Task failed and is being requeued."
    | exitCode, "FAILED" ->
      printfn "Task failed and is not being reattempted."
    | _ -> () // No action needed for other statuses
    let n = data.updateTask (taskEntryMut, connection)
    return statusCodeMeta |> fst
}

let updatePollingInterval (interval: int) =
  let now = DateTime.UtcNow.TimeOfDay
  if now.Hours >= 9 && now.Hours <= 17 then
    min interval minTimeTillNextPollMs
  else
    min (interval * 2) maxTimeTillNextPollMs

let processQueue (cancellationToken: CancellationToken) = async {
  let mutable interval = minTimeTillNextPollMs
  let data = Data(connectionString, queues, taskCount, machineName)
  while not cancellationToken.IsCancellationRequested do
    try
      let connection = data.getConnection()
      let tasks = data.getTasks connection
      match tasks with
      | [] ->
        printfn "Nothing in queue"
        interval <- updatePollingInterval interval
        do! Async.Sleep(interval)
      | _ ->
        let beginOfProcessing = DateTime.UtcNow
        printfn "Processing queue"
        let work = tasks |> List.map (executeWorkItem connection data)
        let! _ = work |> Async.Sequential
        let endOfProcessing = DateTime.UtcNow
        let deltaTimeTillNextPollMs = endOfProcessing - beginOfProcessing
        interval <- minTimeTillNextPollMs
        do! Async.Sleep(max (interval - deltaTimeTillNextPollMs.Milliseconds) 1000)
    with
    | ex ->
      printfn "Error processing queue: %A" ex
      do! Async.Sleep(interval)
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

