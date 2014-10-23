namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Inrush")>]
[<assembly: AssemblyProductAttribute("Inrush")>]
[<assembly: AssemblyDescriptionAttribute("Influx client library for .net.")>]
[<assembly: AssemblyVersionAttribute("0.1")>]
[<assembly: AssemblyFileVersionAttribute("0.1")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.1"
