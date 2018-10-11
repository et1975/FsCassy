module FsCassy.CassandraTests

open System
open FsCassy
open FsCassy.Cassandra
open NUnit.Framework
open Cassandra.Data.Linq
open System.Collections.Generic
open Swensen.Unquote
open System.Diagnostics
open System.Threading

[<CLIMutable>]
type Local = {
    broadcast_address : System.Net.IPAddress
    cluster_name : string
    cql_version : string
    data_center : string
}
let noMapping = Cassandra.Mapping.MappingConfiguration()

[<Test>]
[<Category("interactive")>]
let ``Queryies``() = 
    let cluster = Api.mkCluster "contact points=localhost;default keyspace=system"
    let mkTable () =
        Api.mkTable noMapping (Api.mkSession id cluster)
    
    let interpret = 
        Interpreter.execute mkTable 

    table<Local> >>= read
    |> interpret 
    |> Async.RunSynchronously 
    |> Seq.iter (printf "%A\n")

[<Test>]
[<Category("interactive")>]
let ``Queryies via interface``() = 
    let cluster = Api.mkCluster "contact points=localhost;default keyspace=system"
    let session = Api.mkSession id cluster
        
    let interpreter =  
        { new Interpreter with 
            member x.Interpret<'t,'r> (statement:Statement<'t,Async<'r>>) : Async<'r> = 
                let mkTable () =
                    Api.mkTable noMapping session
                Interpreter.execute mkTable statement }

    async {
        let! one = table<Local> >>= take 1 >>= find |> interpreter.Interpret
        printf "%A\n" one

        let! all = table<Local> >>= read |> interpreter.Interpret 
        all |> Seq.iter (printf "%A\n")

        let! cnt = table<Local> >>= count |> interpreter.Interpret 
        printf "%A\n" cnt
    } |> Async.RunSynchronously
