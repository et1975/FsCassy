module FsCassy.CassandraTests

open System
open FsCassy
open FsCassy.Statement
open FsCassy.Cassandra
open NUnit.Framework
open Cassandra.Data.Linq
open System.Collections.Generic
open Swensen.Unquote
open System.Diagnostics
open System.Threading

[<CLIMutable>]
type Sessions = {
    session_id:Guid
    duration:int
    parameters:Dictionary<string, string>
    request:string
    started_at:DateTimeOffset
}

[<Test>]
[<Category("interactive")>]
let ``Queryies``() = 
    let cluster = Api.mkCluster "contact points=localhost;default keyspace=system_traces"
    let interpret = 
        Interpreter.execute (fun _-> Api.mkTable (Cassandra.Mapping.MappingConfiguration()) (Api.mkSession id cluster)) 

    table<Sessions,_> >>= read
    |> (interpret >> Async.RunSynchronously) 
    |> Seq.iter (printf "%A\n")
