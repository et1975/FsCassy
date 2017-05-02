namespace FsCassy.Cassandra

open System
open System.Linq
open System.Linq.Expressions
open FsCassy


/// Converts F# record updates into C#-like property assignments that
/// the .NET driver expects
module ExpressionTranslator =
    open System.Reflection

    let internal getProperties = memoize(fun (t:Type) -> t.GetProperties(BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.SetProperty).ToDictionary(fun p -> p.Name.ToLower()))

    /// translate f# record init syntax tree into class property init tree: 
    /// r -> v1 -> v5 ... invoke(v5).invoke(v1) 
    /// into:
    /// new { prop1 = v1, prop5 = v5 }
    let translate x:Expression<Func<'T,'T>> = 
        let rec extract (args:Expression seq) (ps:ParameterExpression seq) sx = 
            match box sx with
            | :? LambdaExpression as lambda -> 
                match lambda.Body with
                | :? NewExpression as n ->
                    let props = getProperties typeof<'T>
                    let vals = lambda.Parameters |> Seq.append ps |> Seq.skip 1 // skip $
                    let bindings = 
                        vals.Select(fun p -> props.TryGetValue(p.Name.ToLower())).Where(fun (f,_) -> f)
                        |> Seq.zip args
                        |> Seq.map (fun (a, (_,p)) -> Expression.Bind(p, a))
                        |> Seq.append 
                               (n.Constructor.GetParameters()
                                 .Select(fun p i -> (p, n.Arguments.[i]))
                                 .Where(fun (_, a) -> a.NodeType = ExpressionType.Constant)
                                 .Select(fun (p, a) -> Expression.Bind(props.[p.Name.ToLower()], a)))
                        |> Seq.map (fun b -> b :> MemberBinding)
                        |> Array.ofSeq

                    Expression.Lambda<Func<'T,'T>>(Expression.MemberInit(Expression.New(typeof<'T>),bindings), lambda.Parameters |> Seq.append ps |> Seq.take 1) // take $
                | :? MethodCallExpression as call ->
                    extract (Seq.append args call.Arguments) (Seq.append ps lambda.Parameters) call.Object
                | _ -> failwith "Don't know how to translate the expression"
            | _ -> x
        extract Seq.empty Seq.empty x

/// Convinience helpers on top of official API
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module Api =
    open System.Threading.Tasks
    open Cassandra
    open Cassandra.Data.Linq
    open Cassandra.Mapping

    let mkCluster connStr =
        Cluster
            .Builder()
            .WithConnectionString(connStr)
            .WithReconnectionPolicy(ConstantReconnectionPolicy(100L))
            .WithQueryOptions(QueryOptions().SetConsistencyLevel ConsistencyLevel.LocalOne)
            .Build()

    let mkSession (configure:ISession->ISession) (cluster:ICluster) =
        cluster.Connect()
        |> configure

    let mkTable mappingConfiguration = 
        memoize (fun (session:ISession) -> Table<'t>(session,mappingConfiguration))


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
                q.Select (ExpressionTranslator.translate x) |> (Query >> tuple next >> recurse)
            
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

