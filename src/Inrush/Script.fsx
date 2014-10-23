// Learn more about F# at http://fsharp.org. See the 'F# Tutorial' project
// for more guidance on F# programming.
#load "../../paket-files/mavnn/Algebra.Boolean/Algebra.Boolean/Transforms.fs"
#load "Library.fs"

open Inrush.QueryTypes

let select = 
    SelectStatement({ Columns = 
                          Columns [ Sum(ColumnName "value")
                                    Name(ColumnName "bob") ]
                      Series = Single "test"
                      Where = Some (Equal(ColumnName "bob", String "bob"))
                      GroupBy = Some ([TimeGroup(Days 1);ColumnGroup(ColumnName "bob")] , None) })

select.Show()
