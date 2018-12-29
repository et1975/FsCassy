module FsCassy.InMemTests

open System
open FsCassy
open NUnit.Framework
open Cassandra.Data.Linq
open System.Collections.Generic
open Swensen.Unquote
open System.Diagnostics
open System.Threading


module Samples =
    open Statement

    type X = {x:int}
    type T = {w:string; x:int; y:int; z:int}
    type en = | Zero = 0 | One = 1
    type E = { e:en }

    let exampleWhere : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0)) 
        >>= read

    let exampleWhereNone : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 10)) 
        >>= read

    let exampleFind : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0)) 
        >>= find

    let exampleFindConversion : Statement<E,_> =
            table
        >>= where (Quote.X(fun e -> int e.e = 0 )) 
        >>= find

    let exampleFindNone : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 10)) 
        >>= find

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
        >>= select (Quote.X(fun x -> { x with x = 10 }))
        >>= update
        >>= withConsistency Consistency.Any
        >>= execute

    let exampleUpdateIf : Statement<X,_> =
            table
        >>= where (Quote.X(fun x -> x.x = 0))
        >>= select (Quote.X(fun x -> { x with x = 10 }))
        >>= updateIf (Quote.X(fun x -> x.x = 0))
        >>= withConsistency Consistency.Any
        >>= execute

    let exampleUpdateNew : Statement<T,_> =
            table
        >>= where (Quote.X(fun t -> t.x = 7 && t.y = 8))
        >>= select (Quote.X(fun t -> { t with z = 9 }))
        >>= update
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
let ``InMem reads where``() = 
    Samples.exampleWhere |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously |> List.ofSeq =! [{x=0}; {x=0}]

[<Test>]
let ``InMem reads no-where``() = 
    Samples.exampleWhereNone |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously |> List.ofSeq =! []

[<Test>]
let ``InMem finds where``() = 
    Samples.exampleFind |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously =! Some {x=0}

[<Test>]
let ``InMem finds no-where converted``() = 
    let xs = ResizeArray [ { e = en.One } ]
    Samples.exampleFindConversion |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously =! None

[<Test>]
let ``InMem finds no-where``() = 
    Samples.exampleFindNone |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously =! None

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
let ``InMem updatesNew``() = 
    let xs = ResizeArray [ { w = "0"; x=1; y=2; z=3 }; { w = "0"; x=2; y=3; z=4 }; { w = "0"; x=3; y=4; z=5 } ]
    Samples.exampleUpdateNew |> InMem.Interpreter.execute xs 
    |> Async.RunSynchronously
    xs.Count =! 4
    xs.Contains { w = null; x=7; y=8; z=9 } =! true

[<Test>]
let ``InMem deletes``() = 
    let xs = ResizeArray xs
    Samples.exampleDelete |> InMem.Interpreter.execute xs 
    |> Async.RunSynchronously
    xs.Contains {x=0} =! false
