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

module Async =
    open System
    open System.Threading.Tasks

    let await (t: #Task) = 
        Async.FromContinuations(fun (s, e, c) ->
            t.ContinueWith(fun (t:Task) -> 
                    if t.IsFaulted then e(t.Exception)
                    elif t.IsCompleted then s()
                    else c(System.OperationCanceledException())
                )
            |> ignore
        )

