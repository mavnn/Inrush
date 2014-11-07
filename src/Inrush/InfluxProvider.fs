module Inrush.Influx.Provider

open ProviderImplementation.ProvidedTypes
open Microsoft.FSharp.Core.CompilerServices
open System.Reflection
open FSharp.Data
open Inrush.Influx.Action

type InfluxSeriesDefinition (seriesName, names, values) =
    member x.SeriesName () : string = seriesName
    member x.Columns () : string [] = names
    member x.Values () : string [] = values
    static member Create seriesName names values =
        InfluxSeriesDefinition(seriesName, names, values)

[<TypeProvider>]
type InfluxProvider (cfg : TypeProviderConfig) as this =
    inherit TypeProviderForNamespaces ()
    let ns = "Inrush.Influx.Provider"
    let asm = Assembly.GetExecutingAssembly()
    let assemblyPath = System.IO.Path.ChangeExtension(System.IO.Path.GetTempFileName(), ".dll")
    let tempAssembly = ProvidedAssembly assemblyPath

    let provider = ProvidedTypeDefinition(asm, ns, "Database", None)

    let parameters =
        [
            ProvidedStaticParameter("Server", typeof<string>)
            ProvidedStaticParameter("Database", typeof<string>)
            ProvidedStaticParameter("User", typeof<string>)
            ProvidedStaticParameter("Password", typeof<string>)
        ]

    let getSeriesData (j : JsonValue) =
        j.AsArray()
        |> Array.map (fun s -> s.GetProperty("name").AsString(), s.GetProperty("columns").AsArray() |> Array.map (fun c -> c.AsString()))

    do provider.DefineStaticParameters(parameters,
        fun typeName args ->
            let server = args.[0] :?> string
            let db = args.[1] :?> string
            let u = args.[2] :?> string
            let p = args.[3] :?> string
            let config = { Server = server; Database = db; User = u; Password = p }
            let query = "select * from /.*/ limit 1"

            let dbProvider = ProvidedTypeDefinition(asm, ns, typeName, None)
            let (!!) (s : string) = System.Web.HttpUtility.UrlEncode s
            let uri = sprintf "%s/db/%s/series?u=%s&p=%s&q=%s" config.Server config.Database (!!config.User) (!!config.Password) (!!query)

            let dbJson = JsonValue.Load uri

            let series = getSeriesData dbJson

            let seriesTypes =
                series
                |> Seq.map (fun (name, columns) ->
                    let seriesType = ProvidedTypeDefinition(name, Some typeof<InfluxSeriesDefinition>)
                    let c = ProvidedConstructor([], InvokeCode =
                                fun _ -> 
                                         let size = Array.length columns
                                         <@@ 
                                            InfluxSeriesDefinition.Create name columns (Array.zeroCreate size)
                                         @@>)
                    seriesType.AddMember c
                    columns
                    |> Seq.iteri (fun i c ->
                                    let p = 
                                        ProvidedProperty(
                                            c,
                                            typeof<string>,
                                            GetterCode = (fun args -> <@@
                                                                        ((%%args.[0]:obj) :?> InfluxSeriesDefinition).Values().[i]
                                                                     @@>),
                                            SetterCode = (fun args -> <@@
                                                                        let state = ((%%args.[0]:obj) :?> InfluxSeriesDefinition).Values()
                                                                        state.[i] <- (%%args.[1]:string)
                                                                      @@>))
                                    seriesType.AddMember p)
                    seriesType)

            // tempAssembly.AddTypes <| dbProvider::List.ofSeq seriesTypes
            dbProvider.AddMembers (seriesTypes |> List.ofSeq)
            dbProvider
        )
    do
        // tempAssembly.AddTypes [provider]
        this.AddNamespace(ns, [provider])

[<assembly:TypeProviderAssembly>]
do ()