module FsCassy.InMemTests

open System
open FsCassy
open FsCassy.Statement
open NUnit.Framework
open Cassandra.Data.Linq
open System.Collections.Generic
open Swensen.Unquote
open System.Diagnostics
open System.Threading


module Samples =
    open Statement
    open FSharp

    type X = {x:int}
    let exampleWhere : Statement<X,int,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0)) 
        >>= read

    let exampleTake : Statement<X,int,_> =
            table
        >>= take 1
        >>= read

    let exampleCount : Statement<X,int,_> =
            table
        >>= withConsistency 1
        >>= count

    let exampleUpdate : Statement<X,int,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= select (Quote.X(fun x -> { x with x = 10}))
        >>= update
        >>= withConsistency 1
        >>= execute

    let exampleUpdateIf : Statement<X,int,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= select (Quote.X(fun x -> { x with x = 10}))
        >>= updateIf (Quote.X(fun x -> x.x = 0))
        >>= withConsistency 1
        >>= execute

    let exampleDelete : Statement<X,int,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= delete
        >>= execute

    let exampleUpsert : Statement<X,int,_> =
            table
        >>= upsert {x = 100}
        >>= withConsistency 1
        >>= execute
        


open Samples
let xs = [ {x = 1}
           {x = 2}
           {x = 3}
           {x = 4}
           {x = 0}
           {x = 5}
           {x = 0}
           {x = 6} ]
[<Test>]
let ``InMem reads``() = 
    table >>= read |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously |> List.ofSeq =! xs

[<Test>]
let ``InMem where``() = 
    Samples.exampleWhere |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously |> List.ofSeq =! [{x=0}; {x=0}]

[<Test>]
let ``InMem takes``() = 
    Samples.exampleTake |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously |> List.ofSeq =! [{x=1}]

[<Test>]
let ``InMem counts``() = 
    Samples.exampleCount |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously =! (int64 <| List.length xs)

[<Test>]
let ``InMem upserts``() = 
    let xs = ResizeArray xs
    Samples.exampleUpsert |> InMem.Interpreter.execute xs  
    |> Async.RunSynchronously
    xs.Contains {x=100} =! true

[<Test>]
let ``InMem updates``() = 
    let xs = ResizeArray xs
    Samples.exampleUpdate |> InMem.Interpreter.execute xs 
    |> Async.RunSynchronously
    xs.Contains {x=10} =! true

[<Test>]
let ``InMem updatesIf``() = 
    let xs = ResizeArray xs
    Samples.exampleUpdateIf |> InMem.Interpreter.execute xs 
    |> Async.RunSynchronously
    xs.Contains {x=10} =! true

[<Test>]
let ``InMem deletes``() = 
    let xs = ResizeArray xs
    Samples.exampleDelete |> InMem.Interpreter.execute xs 
    |> Async.RunSynchronously
    xs.Contains {x=0} =! false
