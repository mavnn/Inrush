module Inrush.Tests

open Inrush
open Inrush.QueryTypes
open NUnit.Framework

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
