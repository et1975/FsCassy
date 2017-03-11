namespace FsCassy
open System.Linq.Expressions
open System

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
type Clause<'t,'c,'next> =
    | Table of                                      table: (TableStatement -> 'next)
    | WithConsistency of c: 'c                    * consistency: (Traits.AcceptsConsistency -> 'next)
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

type Statement<'t,'c,'r> =
    | Clause of Clause<'t,'c,Statement<'t,'c,'r>>
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

[<CompilationRepresentation (CompilationRepresentationFlags.ModuleSuffix)>]
module Statement =
    open FSharp

    let rec bindM f =
        function
        | Clause clause -> Clause (Clause.map (bindM f) clause)
        | Exec x -> f x
        
    let returnM x =
        Exec x

    let lift (clause:Clause<'t,'c,_>) : Statement<'t,'c,'r> =
        Clause (Clause.map Exec clause)

    let table<'t,'c> : Statement<'t,'c,_> = lift (Table (fun _ -> TableStatement))
    let take n (_:#Traits.Query) = lift (Take (n, fun _ -> QueryStatement))
    let where x (_:#Traits.Query) = lift (Where(x, fun _ -> QueryStatement))
    let select x (_:#Traits.Query) = lift (Select(x, fun _ -> QueryStatement)) 
    let withConsistency c (q:#Traits.AcceptsConsistency) = lift (WithConsistency (c, fun _ -> q))
    let prepared (s,args) (_:TableStatement) = lift (Prepared (s, args, fun _ -> PreparedStatement))
    let upsert i (_:TableStatement) = lift (Upsert (i, fun _ -> CommandStatement))
    let update (_:#Traits.Query) =  lift (Update (fun _ -> CommandStatement))
    let updateIf x (_:#Traits.Query) = lift (UpdateIf (x, fun _ -> CommandStatement))
    let delete (_:#Traits.Query) = lift (Delete (fun _ -> CommandStatement))
    let count (_:#Traits.Query) = lift (Count id)
    let read (_:#Traits.Readable): Statement<'t,_,_> = lift (Read id)
    let find (_:#Traits.Readable): Statement<'t,_,_> = lift (Find id)
    let execute (_:#Traits.Command): Statement<'t,_,_> = lift (Execute id)

type Statement with
    static member inline (>>=) (x:Statement<'t,'c,_>,f:_->Statement<'t,'c,_>) = 
        Statement.bindM f x

module Printer =
    let internal formatClause : Clause<'t,'c,_> -> _ =
        function
        | Table(mkNext) ->
            ["table", typeof<'t>.Name], mkNext TableStatement
        | WithConsistency(c, mkNext) ->
            ["consistency", (string (box c))], mkNext CommandStatement
        | Take(n, mkNext) -> 
            ["take",(string n)], mkNext QueryStatement
        | Where(x, mkNext) ->
            ["where", (string x)], mkNext QueryStatement
        | Select(x, mkNext) -> 
            ["select", (string x)], mkNext QueryStatement
        | clause -> failwithf "Unxpected clause: %A" clause

    let rec internal interpretClause state =
        function
        | Clause(Find _) ->
            (sprintf "select top 1 from %s" (state |> Map.find "table"))
            + (state |> Map.tryFind "where" |> function Some where -> (sprintf " where %s" where) | _ -> "")
            + (state |> Map.tryFind "take" |> function Some take -> (sprintf " take %s" take) | _ -> "")
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(Count _) ->
            (sprintf "select count(*) from %s" (state |> Map.find "table"))
            + (state |> Map.tryFind "where" |> function Some where -> (sprintf " where %s" where) | _ -> "")
            + (state |> Map.tryFind "take" |> function Some take -> (sprintf " take %s" take) | _ -> "")
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(Read _) ->
            (sprintf "select * from %s" (state |> Map.find "table"))
            + (state |> Map.tryFind "where" |> function Some where -> (sprintf " where %s" where) | _ -> "")
            + (state |> Map.tryFind "take" |> function Some take -> (sprintf " take %s" take) | _ -> "")
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(Delete _) ->
            (sprintf "delete from %s" (state |> Map.find "table"))
            + (state |> Map.tryFind "where" |> function Some where -> (sprintf " where %s" where) | _ -> "")
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(Update _) -> 
            (sprintf "update %s set %s" (state |> Map.find "table") (state |> Map.find "select"))
            + (state |> Map.tryFind "where" |> function Some where -> (sprintf " where %s" where) | _ -> "")
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(UpdateIf (x,_)) -> 
            (sprintf "update %s set %s if %A" (state |> Map.find "table") (state |> Map.find "select") x)
            + (state |> Map.tryFind "where" |> function Some where -> (sprintf " where %s" where) | _ -> "")
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(Upsert (x,_)) -> 
            (sprintf "upsert %A into %s" x (state |> Map.find "table"))
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause(Prepared (s,args,_)) -> 
            (sprintf "%s with args=%A" s args)
            + (state |> Map.tryFind "consistency" |> function Some c -> (sprintf " with consistency %s" c) | _ -> "")
        | Clause clause ->
            let (tokens,next) = formatClause clause
            interpretClause (List.fold (fun acc (k,v) -> Map.add k v acc) state tokens) next
        | stmt -> failwithf "Invalid statement: %A" stmt 

    let interpret statement = interpretClause Map.empty statement
