namespace Inrush

open Algebra.Boolean

[<AutoOpen>]
module Show =
    let inline show x =
        (^a : (member Show : unit -> string) x)

/// Types used to represent the AST of a query
module QueryTypes = 
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
        member x.Show () =
            match x with
            | String s -> sprintf "%A" s
            | Number f -> sprintf "%f" f
            | Time t -> show t

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
                sprintf "(%s and %s)" (show w) (show w')
            | Or (w, w') ->
                sprintf "(%s or %s)" (show w) (show w')

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
