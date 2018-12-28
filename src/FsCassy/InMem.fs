/// InMem implementation for testing
module FsCassy.InMem
open System
open System.Linq
open System.Linq.Expressions

/// Executes statements on a ResizeArray
module Interpreter =
    open System.Collections.Generic
    open System.Runtime.Serialization
    
    type Selection<'t> = 
        | New of 't
        | Existing of int*'t
    module Selection =
        let existing = function | Existing (i,v) -> Some (i,v) | New _ -> None
        let filterExistingValue f = function | New _ -> false | Existing (_,v) -> f v
        let filterExistingIndex f = function | New _ -> false | Existing (i,_) -> f i
        let map f = function | Existing (i,v) -> Existing (i, f v) | New v -> New (f v)
        let iterExisting f = function | Existing (i,v) -> f (i, v) | New v -> ()

    let private mkDefault (x:Expression<Func<'t,bool>>) =
        let destType = x.Parameters.[0].Type
        let rec parseMemberValues : (Expression * Expression) -> (string * obj) list =
            function
            | (:? BinaryExpression as be1), (:? BinaryExpression as be2) ->
                ((be1.Left, be1.Right) |> parseMemberValues) @ ((be2.Left, be2.Right) |> parseMemberValues)

            | (:? MemberExpression as me), (:? ConstantExpression as ce)
            | (:? ConstantExpression as ce), (:? MemberExpression as me) -> [ me.Member.Name, ce.Value ]

            | (:? UnaryExpression as ue), (:? ConstantExpression as ce)
            | (:? ConstantExpression as ce), (:? UnaryExpression as ue) -> (ue.Operand |> unbox<MemberExpression>, ce) |> parseMemberValues // [ (unbox<MemberExpression> ue.Operand).Member.Name, ce.Value ]

            | e1, e2 -> failwith <| sprintf "Unsupported binary expression: %O %O" e1 e2

        let queryValues = match x.Body with | :? BinaryExpression as be -> (be.Left, be.Right) |> parseMemberValues | e -> failwith <| sprintf "Unsupported root expression %O" e
        let initValues =
            FSharp.Reflection.FSharpType.GetRecordFields destType
            |> Array.map (fun pi -> 
                queryValues 
                |> List.tryFind (fun (n, _) -> n = pi.Name)
                |> Option.map snd 
                |> Option.defaultValue (try FormatterServices.GetUninitializedObject pi.PropertyType with | _ -> null))

        unbox<'t> <| FSharp.Reflection.FSharpValue.MakeRecord(destType, initValues)

    [<Struct>]
    type InterpretationState<'t> = 
        | Array of array:'t ResizeArray 
        | Query of query:Async<IEnumerable<'t>>
        | Selection of selection:Async<IEnumerable<Selection<'t>>>
        | Command of command:Async<unit>
        | Done

    let internal (|Querylike|_|) : InterpretationState<'t> -> Async<IEnumerable<'t>> option =
        function
        | Array a -> Some (a :> IEnumerable<'t> |> async.Return)
        | Query q -> Some q
        | _ -> None

    let execute (source:'t ResizeArray) (statement:Statement<'t,'r>) : 'r =
        let rec recurse = function
            | Clause(Table(mkNext)), Array a ->
                let next = mkNext TableStatement
                a |> (Array >> tuple next >> recurse)
            
            | Clause(WithConsistency(c, mkNext)), q ->
                let next = mkNext QueryStatement
                q |> (tuple next >> recurse)
            
            | Clause(Take(n, mkNext)), Querylike q -> 
                let next = mkNext QueryStatement
                async { let! q' = q in return q'.Take n }
                |> (Query >> tuple next >> recurse)
            
            | Clause(Take(n, mkNext)), Selection q -> 
                let next = mkNext QueryStatement
                async { let! q' = q in return q'.Take n }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Where(x, mkNext)), Querylike q ->
                let next = mkNext QueryStatement
                let w = x.Compile().Invoke
                let mkSelection q' = 
                    if q' |> Seq.exists w |> not then mkDefault x |> New |> Seq.singleton 
                    else q' |> Seq.mapi tuple |> Seq.filter (snd >> w) |> Seq.map Existing
                async { let! q' = q in return mkSelection q' }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Where(x, mkNext)), Selection q ->
                let next = mkNext QueryStatement
                let w = x.Compile().Invoke
                async { let! q' = q in return q' |> Seq.filter (Selection.filterExistingValue w) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Select(x, mkNext)), Querylike q -> 
                let next = mkNext QueryStatement
                let sel = x.Compile().Invoke
                async { let! q' = q in return q' |> Seq.mapi (fun i v -> Existing (i,sel v)) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Select(x, mkNext)), Selection q -> 
                let next = mkNext QueryStatement
                let sel = x.Compile().Invoke
                async { let! q' = q in return q' |> Seq.map (Selection.map sel) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(UpdateIf(x, mkNext)), Selection q -> 
                let next = mkNext CommandStatement
                let iff = x.Compile().Invoke
                async { 
                    let! q' = q
                    let res = q' |> Seq.filter (Selection.filterExistingIndex (fun i -> iff source.[i])) |> List.ofSeq
                    res |> List.iter (Selection.iterExisting (fun (i,v) -> source.[i] <- v))
                } |> (Command >> tuple next >> recurse)
            
            | Clause(Update(mkNext)), Selection q -> 
                let next = mkNext CommandStatement
                async { 
                    let! q' = q
                    q' 
                    |> List.ofSeq 
                    |> List.iter (function | Existing (i,v) -> source.[i] <- v | New v -> source.Add v)
                } |> (Command >> tuple next >> recurse)
            
            | Clause(Delete(mkNext)), Querylike q -> 
                let next = mkNext CommandStatement
                async { 
                    let! q' = q
                    let res = q' |> List.ofSeq
                    res |> List.iter (source.Remove >> ignore)
                } |> (Command >> tuple next >> recurse)
            
            | Clause(Delete(mkNext)), Selection q -> 
                let next = mkNext CommandStatement
                async { 
                    let! q' = q
                    let res = q' |> Seq.choose Selection.existing |> Seq.sortByDescending fst |> List.ofSeq
                    res |> List.iter (fst >> source.RemoveAt)
                } |> (Command >> tuple next >> recurse)
            
            | Clause(Upsert(item, mkNext)), Array a ->
                let next = mkNext CommandStatement
                async { a.Add item } 
                |> (Command >> tuple next >> recurse)
            
            | Clause(Execute(mkNext)), Command c ->
                let next = c |> mkNext
                recurse (next,Done) 
            
            | Clause(Count(mkNext)), Querylike q -> 
                let next = async {
                                let! q' = q
                                return q' |> Seq.length |> int64 
                           } |> mkNext
                recurse (next,Done) 
            
            | Clause(Count(mkNext)), Selection q -> 
                let next = async {
                                let! q' = q
                                return q' |> Seq.choose Selection.existing |> Seq.length |> int64 
                           } |> mkNext
                recurse (next,Done) 
            
            | Clause(Read(mkNext)), Querylike q -> 
                let next = q |> mkNext
                recurse (next,Done) 
            
            | Clause(Read(mkNext)), Selection q -> 
                let next = async {
                                let! q' = q
                                return q' |> Seq.choose Selection.existing |> Seq.map snd
                            } |> mkNext
                recurse (next,Done) 
            
            | Clause(Find(mkNext)), Querylike q -> 
                let next = async {
                                let! q' = q 
                                return q' |> Seq.tryHead
                           } |> mkNext
                recurse (next,Done) 
            
            | Clause(Find(mkNext)), Selection q -> 
                let next = async {
                                let! q' = q 
                                return q' |> Seq.choose Selection.existing |> Seq.map snd |> Seq.tryHead
                           } |> mkNext
                recurse (next,Done) 
            
            | Exec x, Done -> 
                x
            
            | statement,_ -> 
                failwithf "Invalid statement: %A" statement

        recurse (statement, Array source)