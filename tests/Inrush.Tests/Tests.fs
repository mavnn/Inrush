module Inrush.Tests

open NUnit.Framework

open Inrush.Influx.AST
open Inrush.Influx.AST.QueryTypes

[<Test>]
let ``Sanity check`` () =
    let select =
        SelectStatement({ Columns = 
                              Columns [ Sum(ColumnName "value")
                                        Name(ColumnName "bob") ]
                          Series = Single "test"
                          Where = Some (Equal(ColumnName "bob", String "bob"))
                          GroupBy = Some ([TimeGroup(Days 1);ColumnGroup(ColumnName "bob")] , None) })
    Assert.AreEqual("SELECT SUM(value), bob FROM \"test\" WHERE bob = \"bob\" GROUP BY time(1d), bob", show select)

open Inrush.Meta

[<Test>]
let ``Merge filters works`` () =
    Assert.DoesNotThrow(fun () -> filterMerge id <@@ Seq.filter (fun x -> true) (Seq.filter (fun y -> true) Seq.empty<string>) @@> |> ignore)

[<Test>]
let ``Merge filters works for multiple filters`` () =
    Assert.DoesNotThrow(fun () -> filterMerge id <@@ Seq.filter (fun z -> true) (Seq.filter (fun x -> true) (Seq.filter (fun y -> true) Seq.empty<string>)) @@> |> ignore)
