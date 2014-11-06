module Inrush.Tests

open NUnit.Framework

open Inrush.Influx
open Inrush.Influx.AST

[<Test>]
let ``Sanity check`` () =
    let select =
        SelectStatement({ Columns = 
                              Columns [ Sum(ColumnName "value")
                                        Name(ColumnName "bob") ]
                          Series = Single "test"
                          Where = Some (Equal(ColumnName "bob", String "bob"))
                          GroupBy = Some ([TimeGroup(Days 1);ColumnGroup(ColumnName "bob")] , None) })
    Assert.AreEqual("SELECT SUM(value), bob FROM \"test\" WHERE bob = 'bob' GROUP BY time(1d), bob", show select)

[<Test>]
let ``Sanity check 2`` () =
    let select =
        SelectStatement({ Columns = 
                              Columns [ As(Sum(ColumnName "value"), "ValueSum")
                                        Name(ColumnName "bob") ]
                          Series = Single "test"
                          Where = Some (Equal(ColumnName "bob", String "bob"))
                          GroupBy = Some ([TimeGroup(Days 1);ColumnGroup(ColumnName "bob")] , None) })
    Assert.AreEqual("SELECT SUM(value) AS ValueSum, bob FROM \"test\" WHERE bob = 'bob' GROUP BY time(1d), bob", show select)

open Inrush.Meta

[<Test>]
let ``Merge filters works`` () =
    Assert.DoesNotThrow(fun () -> filterMerge id <@@ Seq.filter (fun x -> true) (Seq.filter (fun y -> true) Seq.empty<string>) @@> |> ignore)

[<Test>]
let ``Merge filters works for multiple filters`` () =
    Assert.DoesNotThrow(fun () -> filterMerge id <@@ Seq.filter (fun z -> true) (Seq.filter (fun x -> true) (Seq.filter (fun y -> true) Seq.empty<string>)) @@> |> ignore)

open FSharp.Data

let v =
    JsonValue.Parse """[{"name":"Damage","columns":["time","sequence_number","location","amount"],"points":[[1415188487715,20001,"head",10],[1415188475832,10001,"centre torso",22]]}]"""

[<Test>]
let ``Sanity check Json stuff`` () =
    let series = v.AsArray()
    let sut = series.[0].GetProperty("points").AsArray().[0].AsArray().[2].AsString()
    Assert.AreEqual("head", sut)

open Inrush.Influx.AST
open Inrush.Influx.Action

type TestSeries =
    {
        Col1 : string
        Col2 : int
    }

[<Test>]
let ``Create select query`` () =
    Assert.AreEqual(Some "SELECT Col1, Col2 FROM \"TestSeries\"", create <@@ select<TestSeries> () @@>)

[<Test>]
let ``Create select query with filter`` () =
    Assert.AreEqual(Some "SELECT Col1, Col2 FROM \"TestSeries\" WHERE Col1 = 'bob'", create <@@ select<TestSeries> () |> Seq.filter (fun x -> x.Col1 = "bob") @@>)


type Damage =
    {
        amount : int
        location : string
    }

[<Test>]
let ``raw call does what we expect`` () =
    Assert.AreEqual([|{ location = "head"; amount = 10};{ location = "centre torso"; amount = 22 }|], rawCallInflux<Damage> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } "select * from Damage" |> Async.RunSynchronously |> Array.ofSeq)

type Damage' () =
    member val location : string = null with get, set
    member val amount = 0 with get, set

[<Test>]
let ``raw call does what we expect 2`` () =
    let o = Damage'()
    o.location <- "head"
    o.amount <- 10
    let result = rawCallInflux<Damage'> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } "select * from Damage" |> Async.RunSynchronously |> Seq.head
    Assert.AreEqual((o.location, o.amount), (result.location, result.amount))

[<Test>]
let ``Putting it all together`` () =
    let q =
        <@@
            select<Damage>()
            |> Seq.filter (fun x -> x.location = "head")
        @@>
    match get<Damage> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } q with
    | Some result ->
        Assert.AreEqual({ location = "head"; amount = 10}, result |> Async.RunSynchronously |> Seq.head)
    | None ->
        Assert.Fail()