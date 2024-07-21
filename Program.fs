open Acadian.FSharp

type Action = unit -> Async<unit>

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
  do! processQueue ()
  return 0
}

[<EntryPoint>]
let main argv = mainAsync argv |> Async.RunSynchronously
