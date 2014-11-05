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