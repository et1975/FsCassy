module FsCassy.InMemTests

open System
open FsCassy
open NUnit.Framework
open Swensen.Unquote


module Samples =
    open Statement

    type X = {x:int}
    let exampleWhere : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0)) 
        >>= read

    let exampleTake : Statement<X,_> =
            table
        >>= take 1
        >>= read

    let exampleCount : Statement<X,_> =
            table
        >>= withConsistency Consistency.Any
        >>= count

    let exampleUpdate : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= select (Quote.X(fun x -> { x with x = 10}))
        >>= update
        >>= withConsistency Consistency.Any
        >>= execute

    let exampleUpdateIf : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= select (Quote.X(fun x -> { x with x = 10}))
        >>= updateIf (Quote.X(fun x -> x.x = 0))
        >>= withConsistency Consistency.Any
        >>= execute

    let exampleDelete : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= delete
        >>= execute

    let exampleUpsert : Statement<X,_> =
            table
        >>= upsert {x = 100}
        >>= withConsistency Consistency.Any
        >>= execute
        


open Samples
open Hopac

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
    |> run |> List.ofSeq =! xs

[<Test>]
let ``InMem where``() = 
    Samples.exampleWhere |> InMem.Interpreter.execute (ResizeArray xs) 
    |> run |> List.ofSeq =! [{x=0}; {x=0}]

[<Test>]
let ``InMem takes``() = 
    Samples.exampleTake |> InMem.Interpreter.execute (ResizeArray xs) 
    |> run |> List.ofSeq =! [{x=1}]

[<Test>]
let ``InMem counts``() = 
    Samples.exampleCount |> InMem.Interpreter.execute (ResizeArray xs) 
    |> run =! (int64 <| List.length xs)

[<Test>]
let ``InMem upserts``() = 
    let xs = ResizeArray xs
    Samples.exampleUpsert |> InMem.Interpreter.execute xs  
    |> run
    xs.Contains {x=100} =! true

[<Test>]
let ``InMem updates``() = 
    let xs = ResizeArray xs
    Samples.exampleUpdate |> InMem.Interpreter.execute xs 
    |> run
    xs.Contains {x=10} =! true

[<Test>]
let ``InMem updatesIf``() = 
    let xs = ResizeArray xs
    Samples.exampleUpdateIf |> InMem.Interpreter.execute xs 
    |> run
    xs.Contains {x=10} =! true

[<Test>]
let ``InMem deletes``() = 
    let xs = ResizeArray xs
    Samples.exampleDelete |> InMem.Interpreter.execute xs 
    |> run
    xs.Contains {x=0} =! false
