namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Inrush")>]
[<assembly: AssemblyProductAttribute("Inrush")>]
[<assembly: AssemblyDescriptionAttribute("Influx client library for .net.")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
