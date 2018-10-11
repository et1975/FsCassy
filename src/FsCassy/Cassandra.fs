namespace FsCassy.Cassandra

open System
open System.Linq
open System.Linq.Expressions
open FsCassy

module internal Async =
    open System
    open System.Threading.Tasks

    let await (t: #Task) =
        Async.FromContinuations(fun (s, e, c) ->
            t.ContinueWith(fun (t:Task) ->
                    if t.IsFaulted then e(t.Exception)
                    elif t.IsCompleted then s()
                    else c(OperationCanceledException())
                )
            |> ignore
        )


/// Executes the statements using Cassandra Table and Mapper APIs
module Interpreter =
    open System.Threading.Tasks
    open Cassandra
    open Cassandra.Data.Linq
    open Cassandra.Mapping
    open FsCassy

    [<Struct>]
    type internal InterpretationState<'t> = 
        | Table of table:Table<'t>
        | Query of query:CqlQuery<'t>
        | Statement of Cql
        | Command of cmd:CqlCommand
        | Executing

    let internal (|Query|_|) : InterpretationState<'t> -> CqlQuery<'t> option =
        function
        | Table table -> Some (table :> CqlQuery<'t>)
        | Query query -> Some query
        | _ -> None

    let execute mkTable (statement:Statement<'t,'r>) : 'r =
        let table : Table<'t> = mkTable()
        let mapper = table.GetSession() |> Mapper
        let rec recurse = function
            | Clause(Clause.Table(mkNext)), Table t ->
                let next = mkNext TableStatement
                t |> (Table >> tuple next >> recurse)
            
            | Clause(WithConsistency(c, mkNext)), Query q ->
                let next = mkNext QueryStatement
                q.SetConsistencyLevel (Nullable c) |> (Query >> tuple next >> recurse)
            
            | Clause(WithConsistency(c, mkNext)), Command q ->
                let next = mkNext CommandStatement
                q.SetConsistencyLevel (Nullable c) |> (Command >> tuple next >> recurse)
            
            | Clause(WithConsistency(c, mkNext)), Statement cql ->
                let next = mkNext PreparedStatement
                cql.WithOptions (fun o -> o.SetConsistencyLevel c |> ignore) |> (Statement >> tuple next >> recurse)
            
            | Clause(Take(n, mkNext)), Query q -> 
                let next = mkNext QueryStatement
                q.Take n |> (Query >> tuple next >> recurse)
            
            | Clause(Where(x, mkNext)), Query q ->
                let next = mkNext QueryStatement
                q.Where x |> (Query >> tuple next >> recurse)
            
            | Clause(Select(x, mkNext)), Query q -> 
                let next = mkNext QueryStatement
                let exp:Expression<Func<'t,'t>> = ExpressionTranslator.translate x
                q.Select exp |> (Query >> tuple next >> recurse)
            
            | Clause(UpdateIf(x, mkNext)), Query q -> 
                let next = mkNext CommandStatement
                q.UpdateIf x :> CqlCommand |> (Command >> tuple next >> recurse)
            
            | Clause(Update(mkNext)), Query q -> 
                let next = mkNext CommandStatement
                q.Update() :> CqlCommand |> (Command >> tuple next >> recurse)
            
            | Clause(Prepared(s,args,mkNext)), Table t -> 
                let next = mkNext PreparedStatement
                Cql.New(s,args) |> Statement |> tuple next |> recurse
            
            | Clause(Delete(mkNext)), Query q -> 
                let next = mkNext CommandStatement
                q.Delete() :> CqlCommand |> (Command >> tuple next >> recurse)
            
            | Clause(Upsert(item, mkNext)), Table t ->
                let next = mkNext CommandStatement
                t.Insert item :> CqlCommand |> (Command >> tuple next >> recurse)
            
            | Clause(Execute(mkNext)), Command c ->
                let next = c.ExecuteAsync() |> Async.await |> mkNext
                recurse (next,Executing) 
            
            | Clause(Count(mkNext)), Query q -> 
                let next = q.Count().ExecuteAsync() |> Async.AwaitTask |> mkNext
                recurse (next,Executing) 
            
            | Clause(Read(mkNext)), Query q -> 
                let next = q.ExecuteAsync() |> Async.AwaitTask |> mkNext
                recurse (next,Executing) 
            
            | Clause(Find(mkNext)), Query q -> 
                let next = async {
                                let! i = q.FirstOrDefault().ExecuteAsync() |> Async.AwaitTask
                                if Object.ReferenceEquals(i, null) then return None
                                else return Some(i)
                           } |> mkNext
                recurse (next,Executing) 
            
            | Clause(Execute(mkNext)), Statement cql ->
                let next = mapper.ExecuteAsync cql |> Async.await |> mkNext
                recurse (next,Executing) 
            
            | Clause(Read(mkNext)), Statement cql -> 
                let next = mapper.FetchAsync<'t> cql |> Async.AwaitTask |> mkNext
                recurse (next,Executing) 
            
            | Clause(Find(mkNext)), Statement cql -> 
                let next = async {
                                let! i = mapper.FirstOrDefaultAsync<'t> cql |> Async.AwaitTask
                                if Object.ReferenceEquals(i, null) then return None
                                else return Some(i)
                           } |> mkNext
                recurse (next,Executing) 
            
            | Exec x, Executing -> 
                x
            
            | statement,_ -> 
                failwithf "Invalid statement: %A" statement

        recurse (statement, Table table)

