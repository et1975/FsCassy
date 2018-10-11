namespace FsCassy

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

/// Interpreter interface that makes it possible to use the same instance of an interpreter with different type parameters
/// from within the same function.
/// See http://stackoverflow.com/questions/42598677/what-is-the-best-way-to-pass-generic-function-that-resolves-to-multiple-types
type Interpreter =
    abstract member Interpret<'t,'r> : Statement<'t,Computation<'r>> -> Computation<'r>