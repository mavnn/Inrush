module Inrush.Influx.Action
open Inrush.Meta
open Inrush.Influx.AST
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.ExprShape
open Microsoft.FSharp.Quotations.DerivedPatterns
open Algebra.Boolean
open Algebra.Boolean.Simplifiers
open FSharp.Data

type InfluxConfig =
    {
        Server : string
        Database : string
        User : string
        Password : string
    }

// No op, purely used to flag a quotation as an Influx query
let select<'a> () =
    Seq.empty<'a>

let (|InfluxQuery|_|) quote =
    match quote with
    | SpecificCall <@@ Seq.filter @@> (_, _, [ShapeLambda(_, filter);SpecificCall <@@ select @@> (_, [returnType], _)]) ->
        Some (returnType, Some filter)
    | SpecificCall <@@ select @@> (_, [returnType], _) ->
        Some (returnType, None)
    | _ -> None

let (|ColName|_|) quote =
    match quote with
    | PropertyGet (Some (Var _), prop, _) ->
        Some (ColumnName prop.Name)
    | _ -> None

let (|WhereVal|_|) quote =
    match quote with
    | Value (o, t) when t = typeof<bool> ->
        Some <| Bool (o :?> bool)
    | Value (o, t) when t = typeof<string> ->
        Some <| WhereValue.String (o :?> string)
    | Value (o, t) when t = typeof<TimeValue> ->
        Some <| Time(o :?> TimeValue)
    | Value (o, t) when t = typeof<int> ->
        Some <| Number(o :?> int |> float)
    | Value (o, t) when t = typeof<float> ->
        Some <| Number(o :?> float)
    | Value (o, t) ->
        Some <| WhereValue.String (o.ToString())
    | _ ->
        None

let rec buildWhere filter =
    match filter with
    | And' (p, p') ->
        And (buildWhere p, buildWhere p') 
    | Or' (p, p') ->
        Or (buildWhere p, buildWhere p')
    | SpecificCall <@@ (=) @@> (_, _, [ColName left;WhereVal right]) ->
        Equal (left, right)
    | SpecificCall <@@ (<>) @@> (_, _, [ColName left;WhereVal right]) ->
        NotEqual (left, right)
    | SpecificCall <@@ (<) @@> (_, _, [ColName left;WhereVal right]) ->
        LessThan (left, right)
    | SpecificCall <@@ (>) @@> (_, _, [ColName left;WhereVal right]) ->
        Greater (left, right)
    | _ ->
        failwith "Not supported yet."

let getColumns (t : System.Type) =
    t.GetProperties()
    |> Seq.map (fun p -> Name(ColumnName(p.Name)))
    |> List.ofSeq
    |> Columns

let getSeries (t : System.Type) =
    t.Name
    |> SeriesSpec.Single

let buildAST returnType filter =
    {
        Columns = getColumns returnType
        Series = getSeries returnType
        Where = Option.map buildWhere filter
        GroupBy = None
    }

let create quote =
    quote
    |> unpipe
    |> filterMerge beta
    |> function
       | InfluxQuery (t, p) ->
            buildAST t p |> Some
       | _ -> None
    |> Option.map show

let rawCallInflux<'a> config query =
    let t = typeof<'a>
    let (!!) (s : string) = System.Web.HttpUtility.UrlEncode s
    let uri = sprintf "%s/db/%s/series?u=%s&p=%s&q=%s" config.Server config.Database (!!config.User) (!!config.Password) (!!query)
    let props =
        t.GetProperties()
    async {
        let! jv = JsonValue.AsyncLoad uri
        let series = jv.AsArray()
        if Array.length series <> 1 then failwith "We don't support multiple series queries yet."
        let columns =
            series.[0].GetProperty("columns").AsArray()
            |> Array.mapi (fun i x -> i, x.AsString())
            |> Map.ofArray
        let points =
            series.[0].GetProperty("points").AsArray()
        let getValue (x : JsonValue) t' =
            match t' with
            | t' when t' = typeof<string> -> x.AsString() |> box
            | t' when t' = typeof<int> -> x.AsFloat() |> int |> box
            | t' when t' = typeof<float> -> x.AsFloat() |> box
            | t' when t' = typeof<bool> -> x.AsBoolean() |> box
            | _ -> failwith "Sorry, can't deal with that yet"
        let processPointsRecord (ps : JsonValue) =
            let values =
                ps.AsArray()
                |> Array.mapi (fun i x ->
                                    let name = columns.[i]
                                    let index = props |> Seq.tryFindIndex (fun p -> p.Name = name)
                                    match index with
                                    | Some index' ->
                                        let t' = (props |> Seq.find (fun p -> p.Name = name)).PropertyType
                                        let value = getValue x t'
                                        Some (index', value)
                                    | None ->
                                        if name <> "time" && name <> "sequence_number" then failwith "Column name not found in properties"
                                        None)
                |> Array.choose id
                |> Array.sortBy fst
                |> Array.map snd
            FSharpValue.MakeRecord(t, values) :?> 'a
        let processPointsMutable (ps : JsonValue) =
            let ctor = t.GetConstructor(System.Type.EmptyTypes)
            let o = ctor.Invoke(null)
            let values =
                ps.AsArray()
                |> Array.mapi (fun i x ->
                                    let name = columns.[i]
                                    name, x)
                |> Map.ofArray
            props
            |> Seq.iter (fun p ->
                            let set = p.GetSetMethod()
                            set.Invoke(o, [|getValue values.[p.Name] p.PropertyType|]) |> ignore)
            o :?> 'a
        return
            points
            |> Seq.map (if FSharpType.IsRecord t then processPointsRecord else processPointsMutable)
    }

let get<'a> config quote =
    match create quote with
    | Some query ->
        Some (rawCallInflux<'a> config query)
    | None ->
        None
