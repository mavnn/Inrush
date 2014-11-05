module Inrush.Meta

open System
open System.Reflection
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.ExprShape
open Microsoft.FSharp.Quotations.DerivedPatterns
open Algebra.Boolean
open Algebra.Boolean.Simplifiers

let private seqMod =
    (AppDomain.CurrentDomain.GetAssemblies()
    |> Seq.find (fun a -> a.GetName().Name = "FSharp.Core")).GetTypes()
    |> Seq.filter FSharpType.IsModule
    |> Seq.find (fun t -> t.FullName = "Microsoft.FSharp.Collections.SeqModule")

let private filter t =
    let openFilter = seqMod.GetMethod("Filter")
    openFilter.MakeGenericMethod [|t|]

let sizeOf quote =
    let rec inner size q =
        match q with
        | ShapeLambda (_, e) ->
            inner (size + 1) e
        | ShapeVar _ ->
            size + 1
        | ShapeCombination (_, es) ->
            1 + (es |> List.sumBy (inner 0))
    inner 0 quote

let filterMerge reduce quote =
    let rec inner previous =
        let next =
            match previous with
            | SpecificCall <@@ Seq.filter @@> (_, _, [pred1;SpecificCall <@@ Seq.filter @@> (_, types, [pred2;sequence])]) ->
                let seqType = List.head types
                let var = Var("x", seqType)
                let mergedPred =
                    Expr.Lambda(var,
                        Expr.IfThenElse(Expr.Application(pred1, Expr.Var var),
                                        Expr.Application(pred2, Expr.Var var),
                                        Expr.Value false))
                Expr.Call(filter seqType, [mergedPred;sequence])
            | ShapeLambda (v, e) ->
                Expr.Lambda (v, inner e)
            | ShapeVar v ->
                Expr.Var v
            | ShapeCombination (o, es) ->
                RebuildShapeCombination(o, es |> List.map inner)
            |> reduce
        if sizeOf next = sizeOf previous then
            previous
        else
            inner next
    inner quote

