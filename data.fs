module fs_prac.data

open Acadian.FSharp
open Acadian.Dapper.Fs
open FSharp.Control.Tasks.V2.ContextInsensitive
open Dapper

type Item = {
  Id: int
  Description: string option
}

type Data() =
  member this.getConn() = safeSqlConnection "connection-string-stub"

  member this.getItems() = task {
    let x = Unchecked.defaultof<Item>
    let sql = $"
    SELECT
      {nameof x.Id} = i.Id
      ,{nameof x.Description} = i.Description
    FROM dbo.Items AS i
    ;
    "
    use conn = this.getConn()
    let! items = conn.QueryAsync<Item>(sql) |> Task.map (Option.ofObjForce)
    return items |> Option.map Seq.toList
  }

  member this.getItem(id: int) = asyncOption {
    let x = Unchecked.defaultof<Item>
    let sql = $"
    SELECT TOP(1)
      {nameof x.Id} = i.Id
      ,{nameof x.Description} = i.Description
    FROM dbo.Items AS i
    WHERE
      i.Id = @Id
    ;
    "
    use conn = this.getConn()
    let! item = conn.QueryFirstOrDefaultAsync<Item>(sql, {| Id=id |}) |> Task.map (Option.ofObjForce) |> Async.AwaitTask
    return! item
  }

  member this.getItemResult(id: int) = asyncResult {
    let x = Unchecked.defaultof<Item>
    let sql = $"
    SELECT TOP(1)
      {nameof x.Id} = i.Id
      ,{nameof x.Description} = i.Description
    FROM dbo.Items AS i
    WHERE
      i.Id = @Id
    ;
    "
    use conn = this.getConn()
    let! item =
      conn.QueryFirstOrDefaultAsync<Item>(sql, {| Id=id |})
      |> Task.map (Option.ofObjForce)
      |> Async.AwaitTask
      |> Async.map (Result.ofOption "Failed to get the item :(")
    return! item
  }
