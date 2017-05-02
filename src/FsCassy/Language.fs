namespace FsCassy
open System.Linq.Expressions
open System

type Consistency = Cassandra.ConsistencyLevel

module Traits =
    type Readable = interface end
    type Query = inherit Readable
    type Command = interface end
    type AcceptsConsistency = interface end

[<Struct>]
type TableStatement =
    | TableStatement
    interface Traits.Query
    interface Traits.AcceptsConsistency

[<Struct>]
type QueryStatement =
    | QueryStatement
    interface Traits.Query
    interface Traits.AcceptsConsistency

[<Struct>]
type CommandStatement =
    | CommandStatement
    interface Traits.AcceptsConsistency
    interface Traits.Command

[<Struct>]
type PreparedStatement =
    | PreparedStatement
    interface Traits.Readable
    interface Traits.Command
    interface Traits.AcceptsConsistency

//[<Struct>] https://github.com/Microsoft/visualfsharp/issues/1678
type Clause<'t,'next> =
    | Table of                                      table: (TableStatement -> 'next)
    | WithConsistency of c: Consistency           * consistency: (Traits.AcceptsConsistency -> 'next)
    | Take of n: int                              * take: (Traits.Query -> 'next)
    | Where of w: Expression<Func<'t,bool>>       * where: (Traits.Query -> 'next)
    | Select of sel: Expression<Func<'t,'t>>      * select:(Traits.Query -> 'next)
    | UpdateIf of upif: Expression<Func<'t,bool>> * updateIf: (Traits.Command -> 'next)
    | Upsert of i: 't                             * item: (Traits.Command -> 'next)
    | Update of                                     update: (Traits.Command -> 'next)
    | Delete of                                     delete: (Traits.Command -> 'next)
    | Execute of                                    exec: (Async<unit> -> 'next)
    | Count of                                      count: (Async<int64> -> 'next)
    | Read of                                       read: (Async<'t seq> -> 'next)
    | Find of                                       find: (Async<'t option> -> 'next)
    | Prepared of string * obj []                 * prepared: (PreparedStatement -> 'next)

type Statement<'t,'r> =
    | Clause of Clause<'t,Statement<'t,'r>>
    | Exec of 'r

[<CompilationRepresentation (CompilationRepresentationFlags.ModuleSuffix)>]
module Clause =
    let map f =
        function
        | Table mkNext -> Table (f << mkNext)
        | Update mkNext -> Update (f << mkNext)
        | Delete mkNext -> Delete (f << mkNext)
        | WithConsistency(c, mkNext) -> WithConsistency (c, f << mkNext)
        | Take (n, mkNext) -> Take (n, f << mkNext)
        | Where (x, mkNext) -> Where (x, f << mkNext)
        | UpdateIf (x, mkNext) -> UpdateIf (x, f << mkNext)
        | Select (x, mkNext) -> Select (x, f << mkNext)
        | Upsert (i, mkNext) -> Upsert (i, f << mkNext)
        | Execute (mkNext) -> Execute (f << mkNext)
        | Read (mkNext) -> Read (f << mkNext)
        | Find (mkNext) -> Find (f << mkNext)
        | Count (mkNext) -> Count (f << mkNext)
        | Prepared (s, args, mkNext) -> Prepared (s, args, f << mkNext)

/// DSL for composing the statements
[<CompilationRepresentation (CompilationRepresentationFlags.ModuleSuffix)>]
[<AutoOpen>]
module Statement =
    /// Compose statement `x` with the statement returned by `f`
    let rec bindM f =
        function
        | Clause clause -> Clause (Clause.map (bindM f) clause)
        | Exec x -> f x

    /// Return statement `x`
    let returnM x =
        Exec x

    /// Lift `clause` into statement
    let lift (clause:Clause<'t,_>) : Statement<'t,'r> =
        Clause (Clause.map Exec clause)

    /// table starts every statement, binding a mapped type to the operation 
    let table<'t> : Statement<'t,_> = lift (Table (fun _ -> TableStatement))

    /// take query
    let take n (_:#Traits.Query) = lift (Take (n, fun _ -> QueryStatement))

    /// where query
    let where x (_:#Traits.Query) = lift (Where(x, fun _ -> QueryStatement))

    /// select query
    let select x (_:#Traits.Query) = lift (Select(x, fun _ -> QueryStatement))

    /// specify consistency for the operation 
    let withConsistency c (q:#Traits.AcceptsConsistency) = lift (WithConsistency (c, fun _ -> q))

    /// prepared statement
    let prepared (s,args) (_:TableStatement) = lift (Prepared (s, args, fun _ -> PreparedStatement))

    /// insert or update command
    let upsert i (_:TableStatement) = lift (Upsert (i, fun _ -> CommandStatement))
    
    /// partial update command
    let update (_:#Traits.Query) =  lift (Update (fun _ -> CommandStatement))
    
    /// conditional partial update
    let updateIf x (_:#Traits.Query) = lift (UpdateIf (x, fun _ -> CommandStatement))
    
    /// delete command
    let delete (_:#Traits.Query) = lift (Delete (fun _ -> CommandStatement))
    
    /// count the results of a query
    let count (_:#Traits.Query) = lift (Count id)
    
    /// execute read operation (return all matching records)
    let read (_:#Traits.Readable): Statement<'t,_> = lift (Read id)

    /// execute find operation (may return a single record)
    let find (_:#Traits.Readable): Statement<'t,_> = lift (Find id)
    
    /// execute a command statement (doesn't return anything)
    let execute (_:#Traits.Command): Statement<'t,_> = lift (Execute id)



type Statement with
    /// Compose statement `x` with the statement returned by `f`
    static member inline (>>=) (x:Statement<'t,_>,f:_->Statement<'t,_>) = 
        Statement.bindM f x


/// Interpreter interface that makes it possible to use the same instance of an interpreter with different type parameters
/// from within the same function.
/// See http://stackoverflow.com/questions/42598677/what-is-the-best-way-to-pass-generic-function-that-resolves-to-multiple-types
type Interpreter =
    abstract member Interpret<'t,'r> : Statement<'t,'r> -> 'r