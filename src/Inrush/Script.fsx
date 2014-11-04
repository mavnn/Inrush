// Learn more about F# at http://fsharp.org. See the 'F# Tutorial' project
// for more guidance on F# programming.
#load "../../paket-files/mavnn/Algebra.Boolean/Algebra.Boolean/Transforms.fs"
#load "Influx.AST.fs"
#load "Meta.fs"

open Algebra.Boolean.Simplifiers
open Inrush.Influx.AST.QueryTypes
open Inrush.Meta

//let select = 
//    SelectStatement({ Columns = 
//                          Columns [ Sum(ColumnName "value")
//                                    Name(ColumnName "bob") ]
//                      Series = Single "test"
//                      Where = Some (Equal(ColumnName "bob", String "bob"))
//                      GroupBy = Some ([TimeGroup(Days 1);ColumnGroup(ColumnName "bob")] , None) })
//
//select.Show()

type MyQueryResult =
    {
        value : int
        bob : string
    }

let get<'a> () =
    Seq.empty<'a>

let s =
    <@
        fun name value ->
            get<MyQueryResult> ()
            |> Seq.filter (fun x -> x.bob = name)
            |> Seq.filter (fun x -> x.value = value || x.bob = "fred")
    @>

let betad =
    beta <@ (%s : string -> int -> seq<MyQueryResult>) "bob" 10 @>

let s' =
    <@@
        get<MyQueryResult> ()
        |> Seq.filter (fun x -> x.bob = "bob")
        |> Seq.filter (fun x -> x.value = 100 || x.bob = "fred")
        |> Seq.filter (fun x -> x.bob = "fred")
    @@>

let one = unpipe s'

let two =
    unpipe s'
    |> filterMerge id
