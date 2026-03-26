#!/usr/bin/env -S dotnet fsi
#r "nuget: Fake.Core.Target, 5.23.1"
#r "nuget: Fake.IO.FileSystem, 5.23.1"
#r "nuget: Fake.DotNet.Cli, 5.23.1"
#r "nuget: Fake.Core.ReleaseNotes, 5.23.1"
#r "nuget: Fake.Tools.Git, 5.23.1"
#r "nuget: MSBuild.StructuredLogger, 2.2.441"

open Fake.Core
open Fake.Core.TargetOperators
open Fake.DotNet
open Fake.Tools
open Fake.IO
open Fake.IO.FileSystemOperators
open Fake.IO.Globbing.Operators
open System
open System.IO

let project = "FsCassy"
let summary = "Functional F# API for Cassandra"
let gitOwner = "et1975"
let gitHome = sprintf "https://github.com/%s" gitOwner
let gitName = "FsCassy"

let packages = ["FsCassy"; "FsCassy.Hopac"]

let projects =
    !! "src/**/*.fsproj"

// --------------------------------------------------------------------------------------
// FAKE context setup

System.Environment.GetCommandLineArgs()
|> Array.skip 2 // fsi.exe; build.fsx
|> Array.toList
|> Context.FakeExecutionContext.Create false __SOURCE_FILE__
|> Context.RuntimeContext.Fake
|> Context.setExecutionContext

let release = ReleaseNotes.load "RELEASE_NOTES.md"

// --------------------------------------------------------------------------------------
// Targets

Target.create "Clean" (fun _ ->
    packages
    |> List.collect (fun n -> [sprintf "src/%s/obj" n; sprintf "src/%s/bin" n])
    |> List.append [
        "src/FsCassy.Tests/obj"; "src/FsCassy.Tests/bin"
        "src/FsCassy.Hopac.Tests/obj"; "src/FsCassy.Hopac.Tests/bin"
    ]
    |> List.iter Shell.cleanDir
)

Target.create "Meta" (fun _ ->
    [ "<Project xmlns=\"http://schemas.microsoft.com/developer/msbuild/2003\">"
      "<PropertyGroup>"
      sprintf "<Description>%s</Description>" summary
      sprintf "<PackageProjectUrl>%s/%s</PackageProjectUrl>" gitHome gitName
      "<PackageLicenseExpression>Apache-2.0</PackageLicenseExpression>"
      sprintf "<RepositoryUrl>%s/%s.git</RepositoryUrl>" gitHome gitName
      sprintf "<PackageReleaseNotes>%s</PackageReleaseNotes>" (System.Security.SecurityElement.Escape(List.head release.Notes))
      "<PackageTags>fsharp;cassandra</PackageTags>"
      "<Authors>et1975</Authors>"
      sprintf "<Version>%s</Version>" (string release.SemVer)
      "</PropertyGroup>"
      "<ItemGroup>"
      "<PackageReference Include=\"Microsoft.SourceLink.GitHub\" Version=\"1.0.0\" PrivateAssets=\"All\"/>"
      "</ItemGroup>"
      "</Project>"]
    |> File.write false "Directory.Build.props"
)

Target.create "Restore" (fun _ ->
    projects
    |> Seq.iter (Path.GetDirectoryName >> DotNet.restore id)
)

Target.create "Build" (fun _ ->
    projects
    |> Seq.iter (Path.GetDirectoryName >> DotNet.build id)
)

Target.create "Test" (fun _ ->
    !! "src/**/*Tests.fsproj"
    |> Seq.iter (fun proj ->
        DotNet.test (fun p -> { p with Filter = Some "TestCategory!=interactive" }) (Path.GetDirectoryName proj))
)

Target.create "TestInteractive" (fun _ ->
    !! "src/**/*Tests.fsproj"
    |> Seq.iter (fun proj ->
        DotNet.test (fun p -> { p with Filter = Some "TestCategory=interactive" }) (Path.GetDirectoryName proj))
)

Target.create "Package" (fun _ ->
    packages
    |> List.iter (fun n -> DotNet.pack id (sprintf "src/%s" n))
)

Target.create "PublishNuget" (fun _ ->
    let exec dir = DotNet.exec (DotNet.Options.withWorkingDirectory dir)
    packages
    |> List.iter (fun n ->
        let args = sprintf "push %s.%s.nupkg -s nuget.org -k %s" n (string release.SemVer) (Environment.environVar "nugetkey")
        let result = exec (sprintf "src/%s/bin/Release" n) "nuget" args
        if (not result.OK) then failwithf "%A" result.Errors)
)

// --------------------------------------------------------------------------------------
// Documentation

Target.create "GenerateDocs" (fun _ ->
    let res = Shell.Exec("dotnet", "tool restore")
    if res <> 0 then failwith "Failed to restore dotnet tools"
    let res = Shell.Exec("dotnet", "fsdocs build")
    if res <> 0 then failwith "Failed to generate docs"
)

Target.create "WatchDocs" (fun _ ->
    let res = Shell.Exec("dotnet", "tool restore")
    if res <> 0 then failwith "Failed to restore dotnet tools"
    let res = Shell.Exec("dotnet", "fsdocs watch")
    if res <> 0 then failwithf "Failed to watch docs: %d" res
)

Target.create "Publish" ignore

// --------------------------------------------------------------------------------------
// Build order

"Clean"
    ==> "Meta"
    ==> "Restore"
    ==> "Build"
    ==> "Test"
    ==> "Package"
    ==> "PublishNuget"
    ==> "Publish"

// start build
Target.runOrDefault "Test"
