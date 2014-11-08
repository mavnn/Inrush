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

open FSharp.Data

let v =
    JsonValue.Parse """[{"name":"Damage","columns":["time","sequence_number","location","amount"],"points":[[1415188487715,20001,"head",10],[1415188475832,10001,"centre torso",22]]}]"""

[<Test>]
let ``Sanity check Json stuff`` () =
    let series = v.AsArray()
    let sut = series.[0].GetProperty("points").AsArray().[0].AsArray().[2].AsString()
    Assert.AreEqual("head", sut)

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


type Damage =
    {
        amount : int
        location : string
    }

// These integration tests will only run if you have my personal influx vm...
//[<Test>]
//let ``raw call does what we expect`` () =
//    Assert.AreEqual([|{ location = "head"; amount = 10};{ location = "centre torso"; amount = 22 }|], rawCallInflux<Damage> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } "select * from Damage" |> Async.RunSynchronously |> Seq.skip 1 |> Array.ofSeq)
//
//type Damage' () =
//    member val location : string = null with get, set
//    member val amount = 0 with get, set
//
//[<Test>]
//let ``raw call does what we expect 2`` () =
//    let o = Damage'()
//    o.location <- "head"
//    o.amount <- 10
//    let result = rawCallInflux<Damage'> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } "select * from Damage" |> Async.RunSynchronously |> Seq.skip 1 |> Seq.head
//    Assert.AreEqual((o.location, o.amount), (result.location, result.amount))
//
//[<Test>]
//let ``Putting it all together`` () =
//    let q =
//        <@@
//            select<Damage>()
//            |> Seq.filter (fun x -> x.location = "head")
//        @@>
//    match get<Damage> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } q with
//    | Some result ->
//        Assert.AreEqual({ location = "head"; amount = 10}, result |> Async.RunSynchronously |> Seq.head)
//    | None ->
//        Assert.Fail()
//
//[<Test>]
//let ``Putting it all together 2`` () =
//    let func =
//        <@
//            fun loc ->
//                select<Damage>()
//                |> Seq.filter (fun x -> x.location = loc)
//        @>
//    let q = <@@ (%func) "head" @@>
//    match get<Damage> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } q with
//    | Some result ->
//        Assert.AreEqual({ location = "head"; amount = 10}, result |> Async.RunSynchronously |> Seq.head)
//    | None ->
//        Assert.Fail()
//
//[<Test>]
//let ``Putting it all together 3`` () =
//    let func =
//        <@
//            fun loc filter ->
//                select<Damage>()
//                |> Seq.filter (fun x -> x.location = loc)
//                |> Seq.filter filter
//        @>
//    let filter = <@ fun (x : Damage) -> x.amount = 10 @>
//    let q = <@@ (%func) "head" (%filter) @@>
//    match get<Damage> { Server = "http://localhost:8086"; Database = "bt"; User = "root"; Password = "root" } q with
//    | Some result ->
//        Assert.AreEqual({ location = "head"; amount = 10}, result |> Async.RunSynchronously |> Seq.head)
//    | None ->
//        Assert.Fail()

// Provider stuff
let seriesJson = """[{"name":"Damage","columns":["time","sequence_number","location","amount"],"points":[[1415276257320,10001,"centre torso",20]]},{"name":"grafana.dashboard_R3JhZmFuYQ==","columns":["time","sequence_number","title","tags","dashboard"],"points":[[1000000000000,1,"Grafana","","{\"title\":\"Grafana\",\"tags\":[],\"style\":\"dark\",\"timezone\":\"browser\",\"editable\":true,\"rows\":[{\"title\":\"Welcome to Grafana\",\"height\":\"210px\",\"editable\":true,\"collapse\":false,\"collapsable\":true,\"panels\":[{\"error\":false,\"span\":6,\"editable\":true,\"type\":\"text\",\"loadingEditor\":false,\"mode\":\"html\",\"content\":\"\u003cbr/\u003e\\n\\n\u003cdiv class=\\\"row-fluid\\\"\u003e\\n  \u003cdiv class=\\\"span6\\\"\u003e\\n    \u003cul\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs#configuration\\\" target=\\\"_blank\\\"\u003eConfiguration\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/troubleshooting\\\" target=\\\"_blank\\\"\u003eTroubleshooting\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/support\\\" target=\\\"_blank\\\"\u003eSupport\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/features/intro\\\" target=\\\"_blank\\\"\u003eGetting started\u003c/a\u003e  (Must read!)\\n      \u003c/li\u003e\\n    \u003c/ul\u003e\\n  \u003c/div\u003e\\n  \u003cdiv class=\\\"span6\\\"\u003e\\n    \u003cul\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/features/graphing\\\" target=\\\"_blank\\\"\u003eGraphing\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/features/annotations\\\" target=\\\"_blank\\\"\u003eAnnotations\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/features/graphite\\\" target=\\\"_blank\\\"\u003eGraphite\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/features/influxdb\\\" target=\\\"_blank\\\"\u003eInfluxDB\u003c/a\u003e\\n      \u003c/li\u003e\\n      \u003cli\u003e\\n        \u003ca href=\\\"http://grafana.org/docs/features/opentsdb\\\" target=\\\"_blank\\\"\u003eOpenTSDB\u003c/a\u003e\\n      \u003c/li\u003e\\n    \u003c/ul\u003e\\n  \u003c/div\u003e\\n\u003c/div\u003e\",\"style\":{},\"title\":\"Documentation Links\"},{\"error\":false,\"span\":6,\"editable\":true,\"type\":\"text\",\"mode\":\"html\",\"content\":\"\u003cbr/\u003e\\n\\n\u003cdiv class=\\\"row-fluid\\\"\u003e\\n  \u003cdiv class=\\\"span12\\\"\u003e\\n    \u003cul\u003e\\n      \u003cli\u003eCtrl+S saves the current dashboard\u003c/li\u003e\\n      \u003cli\u003eCtrl+F Opens the dashboard finder\u003c/li\u003e\\n      \u003cli\u003eCtrl+H Hide/show row controls\u003c/li\u003e\\n      \u003cli\u003eClick and drag graph title to move panel\u003c/li\u003e\\n      \u003cli\u003eHit Escape to exit graph when in fullscreen or edit mode\u003c/li\u003e\\n      \u003cli\u003eClick the colored icon in the legend to change series color\u003c/li\u003e\\n      \u003cli\u003eCtrl or Shift + Click legend name to hide other series\u003c/li\u003e\\n    \u003c/ul\u003e\\n  \u003c/div\u003e\\n\u003c/div\u003e\\n\",\"style\":{},\"title\":\"Tips \u0026 Shortcuts\"}],\"notice\":false},{\"title\":\"New row\",\"height\":\"250px\",\"editable\":true,\"collapse\":false,\"panels\":[{\"span\":12,\"editable\":true,\"type\":\"graph\",\"loadingEditor\":false,\"datasource\":null,\"renderer\":\"flot\",\"x-axis\":true,\"y-axis\":true,\"scale\":1,\"y_formats\":[\"short\",\"short\"],\"grid\":{\"leftMax\":null,\"rightMax\":null,\"leftMin\":null,\"rightMin\":null,\"threshold1\":null,\"threshold2\":null,\"threshold1Color\":\"rgba(216, 200, 27, 0.27)\",\"threshold2Color\":\"rgba(234, 112, 112, 0.22)\"},\"annotate\":{\"enable\":false},\"resolution\":100,\"lines\":true,\"fill\":0,\"linewidth\":1,\"points\":false,\"pointradius\":5,\"bars\":false,\"stack\":false,\"legend\":{\"show\":true,\"values\":false,\"min\":false,\"max\":false,\"current\":false,\"total\":false,\"avg\":false},\"percentage\":false,\"zerofill\":true,\"nullPointMode\":\"connected\",\"steppedLine\":false,\"tooltip\":{\"value_type\":\"cumulative\",\"query_as_alias\":true},\"targets\":[{\"function\":\"mean\",\"column\":\"value\"}],\"aliasColors\":{},\"aliasYAxis\":{},\"title\":\"A graph\"}]}],\"pulldowns\":[{\"type\":\"filtering\",\"collapse\":false,\"notice\":false,\"enable\":false},{\"type\":\"annotations\",\"enable\":false}],\"nav\":[{\"type\":\"timepicker\",\"collapse\":false,\"notice\":false,\"enable\":true,\"status\":\"Stable\",\"time_options\":[\"5m\",\"15m\",\"1h\",\"6h\",\"12h\",\"24h\",\"2d\",\"7d\",\"30d\"],\"refresh_intervals\":[\"5s\",\"10s\",\"30s\",\"1m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"1d\"],\"now\":true}],\"time\":{\"from\":\"now-6h\",\"to\":\"now\"},\"templating\":{\"list\":[]},\"version\":2}"]]}]"""

let getSeriesData (j : JsonValue) =
    j.AsArray()
    |> Array.map (fun s -> s.GetProperty("name").AsString(), s.GetProperty("columns").AsArray() |> Array.map (fun c -> c.AsString()))

[<Test>]
let ``Get series data`` () =
    let j = JsonValue.Parse seriesJson
    let seriesName = getSeriesData j |> Seq.head |> fst
    Assert.AreEqual("Damage", seriesName)

[<Test>]
let ``Get series data 2`` () =
    let j = JsonValue.Parse seriesJson
    let seriesName = getSeriesData j |> Seq.head |> snd
    Assert.AreEqual([|"time";"sequence_number";"location";"amount"|], seriesName)
