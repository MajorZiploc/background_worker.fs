// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp

open System
open Acadian.FSharp

// record
type PersonR = {
  name: string;
  age: int;
}
with
  // string interpolation
  member this.say () = $"Hi, my name is {this.name} and I am {this.age} years old in method form"

module PersonR =
  let say (p: PersonR) = $"Hi, my name is {p.name} and I am {p.age} years old in module function form"

let recordPrac () =
  let p = { name="Bob"; age=24; }
  printfn "%s" (p.say())
  // Update field on record based on old record data
  let p = { p with age=p.age + 10; }
  p |> PersonR.say |> printfn "%s"

// union type
type IPAddressFormat = 
  | V4 of (int*int*int*int)
  | V6

module IPAddressFormat =
  // short hand pattern matching
  let show = function
    | V4(nums) -> printfn "V4 of %A" nums
    | V6 -> printfn "V6"


let setPrac () = 
  let l1 = [1; 2; 3]
  let l2 = [2; 3; 4]
  let _s1 = Set.intersectMany [(Set.ofList l1); (Set.ofList l2)]
  // same as last but only for 2 sets rather than a list
  let s1 = Set.intersect (Set.ofList l1) (Set.ofList l2)
  printfn "%A" s1
  let s2 = Set.union (Set.ofList l1) (Set.ofList l2)
  printfn "%A" s2

let mapPrac () =
  let m1 = Map.ofList [1, "one"; 2, "two"]
  printfn "%A" m1
  let v1 = m1 |> Map.tryFind 1
  printfn "%A" v1
  m1 |> Map.iter (printfn "%A -> %A")
  printfn "%A" (m1 |> Map.toList)

// Define a function to construct a message to print
let from whom =
  sprintf "from %s" whom

let mainAsync _argv = async {
  let a = async { return 1 }
  let xs = [a; a]
  let axs = xs |> Async.Sequential
  let! _axs = axs |> Async.map (printfn "%A")
  IPAddressFormat.show IPAddressFormat.V6
  IPAddressFormat.V4 (1,2,3,4) |> IPAddressFormat.show
  setPrac ()
  recordPrac ()
  mapPrac ()
  let message = from "F#" // Call the function
  printfn "Hello world %s" message
  return 0 // return an integer exit code
}

[<EntryPoint>]
let main argv = mainAsync argv |> Async.RunSynchronously

