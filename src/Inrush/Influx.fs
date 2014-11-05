namespace Inrush.Influx

[<AutoOpen>]
module Show =
    let inline show x =
        (^a : (member Show : unit -> string) x)

/// Types used to represent the AST of a query
module AST = 
    type ColumnName =
        | ColumnName of string
        member x.Show() =
            match x with ColumnName n -> n

    type Column =
        | Name of ColumnName
        | Count of ColumnName
        | Min of ColumnName
        | Max of ColumnName
        | Mean of ColumnName
        | Mode of ColumnName
        | Median of ColumnName
        | Distinct of ColumnName
        | Percentile of ColumnName * double
        | Histogram of ColumnName * double option
        | Derivative of ColumnName
        | Sum of ColumnName
        | As of Column * string
        member x.Show () =
            match x with
            | Name s -> sprintf "%s" <| s.Show()
            | Count c -> sprintf "COUNT(%s)" <| c.Show()
            | Min c -> sprintf "MIN(%s)" <| c.Show()
            | Max c -> sprintf "MAX(%s)" <| c.Show()
            | Mean c -> sprintf "MEAN(%s)" <| c.Show()
            | Mode c -> sprintf "MODE(%s)" <| c.Show()
            | Median c -> sprintf "MEDIAN(%s)" <| c.Show()
            | Distinct c -> sprintf "DISTINCT(%s)" <| c.Show()
            | Percentile (c, f) -> sprintf "PERCENTILE(%s, %f)" (c.Show()) f
            | Histogram (c, sf) ->
                match sf with
                | Some f -> sprintf "HISTOGRAM(%s, %f)" (c.Show()) f
                | None -> sprintf "HISTOGRAM(%s)" (c.Show())
            | Derivative c -> sprintf "DERIVATIVE(%s)" (c.Show())
            | Sum c -> sprintf "SUM(%s)" (c.Show())
            | As (c, name) -> sprintf "%s AS %s" (c.Show()) name

    type ColumnSpec =
        | Star
        | Columns of Column list
        member x.Show () =
            match x with
            | Star -> "*"
            | Columns cl ->
                cl
                |> Seq.map (fun c -> c.Show())
                |> String.concat ", "

    type SeriesSpec =
        | Single of string
        | RegEx of string
        member x.Show () =
            match x with
            | Single s -> sprintf "\"%s\"" s
            | RegEx s -> sprintf "/%s/" s

    type TimeInterval =
        | Microseconds of int
        | Seconds of int
        | Minutes of int
        | Hours of int
        | Days of int
        | Weeks of int
        member x.Show () =
            let f = sprintf "%d%s"
            match x with
            | Microseconds i -> f i "u"
            | Seconds i -> f i "s"
            | Minutes i -> f i "m"
            | Hours i -> f i "h"
            | Days i -> f i "d"
            | Weeks i -> f i "w"

    type TimeValue =
        | Now
        | TimeInt of TimeInterval
        | Minus of TimeValue * TimeValue
        | Plus of TimeValue * TimeValue
        member x.Show () =
            match x with
            | Now -> "now()"
            | TimeInt t -> show t
            | Minus (t, t') -> sprintf "(%s - %s)" (show t) (show t')
            | Plus (t, t') -> sprintf "(%s - %s)" (show t) (show t')

    type WhereValue =
        | String of string
        | Number of float
        | Time of TimeValue
        | Bool of bool
        member x.Show () =
            match x with
            | String s -> sprintf "'%s'" s
            | Number f -> sprintf "%f" f
            | Time t -> show t
            | Bool b -> sprintf "%A" b

    type WhereClause =
        | Equal of ColumnName * WhereValue
        | NotEqual of ColumnName * WhereValue
        | Like of ColumnName * WhereValue
        | NotLike of ColumnName * WhereValue
        | Greater of ColumnName * WhereValue
        | LessThan of ColumnName * WhereValue
        | And of WhereClause * WhereClause
        | Or of WhereClause * WhereClause
        member x.Show () =
            match x with
            | Equal (c, w) ->
                sprintf "%s = %s" (show c) (show w)
            | NotEqual (c, w) ->
                sprintf "%s <> %s" (show c) (show w)
            | Like (c, w) ->
                sprintf "%s =~ %s" (show c) (show w)
            | NotLike (c, w) ->
                sprintf "%s !~ %s" (show c) (show w)
            | Greater (c, w) ->
                sprintf "%s > %s" (show c) (show w)
            | LessThan (c, w) ->
                sprintf "%s < %s" (show c) (show w)
            | And (w, w') ->
                sprintf "(%s AND %s)" (show w) (show w')
            | Or (w, w') ->
                sprintf "(%s OR %s)" (show w) (show w')

    type GroupClause =
        | ColumnGroup of ColumnName
        | TimeGroup of TimeInterval
        member x.Show () =
            match x with
            | ColumnGroup c ->
                show c
            | TimeGroup t ->
                sprintf "time(%s)" (show t)

    type Fill =
        | FillValue of WhereValue
        member x.Show () =
            match x with FillValue f -> show f

    type Select<'a> =
        {
            Columns : ColumnSpec
            Series : SeriesSpec
            Where : WhereClause option
            GroupBy : (GroupClause list * Fill option) option
        }
        member x.Show () =
            let where =
                match x.Where with
                | Some w ->
                    sprintf " WHERE %s" (show w)
                | None -> ""
            let group =
                match x.GroupBy with
                | Some (gc, sf) ->
                    let fill =
                        match sf with
                        | Some s ->
                            sprintf " fill(%s)" (show s)
                        | None -> ""
                    let groups =
                        gc
                        |> List.map show
                        |> String.concat ", "
                    sprintf " GROUP BY %s%s" groups fill
                | None -> ""
            sprintf "SELECT %s FROM %s%s%s" (show x.Columns) (show x.Series) where group

    type Query<'a> =
        | SelectStatement of Select<'a>
        member x.Show () =
            match x with
            | SelectStatement s ->
                s.Show()

module Action =
    open Inrush.Meta
    open AST
    open System
    open System.Reflection
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Algebra.Boolean
    open Algebra.Boolean.Simplifiers
    
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
        |> filterMerge (beta >> unbind >> complement >> idempotence)
        |> function
           | InfluxQuery (t, p) ->
                buildAST t p |> Some
           | _ -> None
        |> Option.map show
