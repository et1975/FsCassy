[<AutoOpen>]
module FsCassy.Prelude
open System
open System.Linq.Expressions

let tuple x y = (x,y)

let memoize f =
    let dict = System.Collections.Concurrent.ConcurrentDictionary<_,_>()
    fun x -> dict.GetOrAdd(x, lazy (f x)).Force()

/// compiler coercion to produce Linq expressions from F# lambdas
type Quote<'T> = 
    static member X(exp:Expression<Func<'T,'a>>) = exp

module Job =
    open Hopac

    let inline await t = t |> (Job.awaitTask >> Job.Ignore)  
        

