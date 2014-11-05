// Learn more about F# at http://fsharp.org. See the 'F# Tutorial' project
// for more guidance on F# programming.
#load "../../paket-files/mavnn/Algebra.Boolean/Algebra.Boolean/Transforms.fs"
#load "Meta.fs"
#load "Influx.fs"

open Algebra.Boolean.Simplifiers
open Inrush.Influx.AST
open Inrush.Influx.Action
open Inrush.Meta
open Inrush.Influx

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

let s =
    <@
        fun name value ->
            select<MyQueryResult> ()
            |> Seq.filter (fun x -> x.bob = name)
            |> Seq.filter (fun x -> x.value = value || x.bob = "fred")
    @>

let betad =
    beta <@ (%s : string -> int -> seq<MyQueryResult>) "bob" 10 @>

let s' =
    <@@
        select<MyQueryResult> ()
        |> Seq.filter (fun x -> x.bob = "bob")
        |> Seq.filter (fun x -> x.value = 100 || x.bob = "fred")
        |> Seq.filter (fun x -> x.bob = "fred")
    @@>

let one = unpipe s'

type Damage =
    {
        amount : int
        location : string
    }

let q =
    <@@
        select<Damage> ()
        |> Seq.filter (fun x -> x.location = "centre torso")
        |> Seq.filter (fun x -> x.location <> "head")
        |> Seq.filter (fun x -> x.amount > 11)
    @@>
    |> create

//val q : string option =
//  Some
//    "SELECT amount, location FROM "Damage" WHERE ((amount > 11.000000 AND location <> 'head') AND location = 'centre torso')"
