# background_worker.fs

A basic implementation of a worker that uses a postgresql table as a queue

# Examples

## background_task that consumes the json payload from the encoded base64 to a model

```fsharp
open System
open Newtonsoft.Json
open Newtonsoft.Json.Linq

let base64ToString (base64: string) =
  let bytes = Convert.FromBase64String(base64)
  Encoding.UTF8.GetString(bytes)

type InputItem = {
  Id: string
  Name: string option
}

type InputPayload = {
  Items: InputItem
}

[<EntryPoint>]
let main argv =
  let jsonStr = argv.[0] |> base64ToString
  let inputPayload = JsonConvert.DeserializeObject<InputPayload>(jsonStr)
  printfn "inputPayload: %A" inputPayload
  0
```
