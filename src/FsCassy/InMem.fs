module FsCassy.InMem
open System
open System.Linq
open System.Linq.Expressions
open System.Threading.Tasks

/// Executes statements on a ResizeArray
module Interpreter =
    open System.Threading.Tasks
    open System.Collections.Generic

    [<Struct>]
    type InterpretationState<'t> = 
        | Array of array:'t ResizeArray 
        | Query of query:Async<IEnumerable<'t>>
        | Selection of selection:Async<IEnumerable<int*'t>>
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
                async { let! q' = q in return q' |> Seq.mapi tuple |> Seq.filter (snd >> w) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Where(x, mkNext)), Selection q ->
                let next = mkNext QueryStatement
                let w = x.Compile().Invoke
                async { let! q' = q in return q' |> Seq.filter (snd >> w) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Select(x, mkNext)), Querylike q -> 
                let next = mkNext QueryStatement
                let sel = x.Compile().Invoke
                async { let! q' = q in return q' |> Seq.mapi (fun i v -> i,sel v) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(Select(x, mkNext)), Selection q -> 
                let next = mkNext QueryStatement
                let sel = x.Compile().Invoke
                async { let! q' = q in return q' |> Seq.map (fun (i,v) -> i,sel v) }
                |> (Selection >> tuple next >> recurse)
            
            | Clause(UpdateIf(x, mkNext)), Selection q -> 
                let next = mkNext CommandStatement
                let iff = x.Compile().Invoke
                async { 
                    let! q' = q
                    let res = q' |> Seq.filter (fun (i,_) -> iff source.[i]) |> List.ofSeq
                    res |> List.iter (fun (i,v) -> source.[i] <- v)
                } |> (Command >> tuple next >> recurse)
            
            | Clause(Update(mkNext)), Selection q -> 
                let next = mkNext CommandStatement
                async { 
                    let! q' = q
                    q' |> List.ofSeq |> List.iter (fun (i,v) -> source.[i] <- v)
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
                    let res = q' |> Seq.sortByDescending fst |> List.ofSeq
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
                                return q' |> Seq.length |> int64 
                           } |> mkNext
                recurse (next,Done) 
            
            | Clause(Read(mkNext)), Querylike q -> 
                let next = q |> mkNext
                recurse (next,Done) 
            
            | Clause(Read(mkNext)), Selection q -> 
                let next = async {
                                let! q' = q
                                return q' |> Seq.map snd
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
                                return q' |> Seq.map snd |> Seq.tryHead
                           } |> mkNext
                recurse (next,Done) 
            
            | Exec x, Done -> 
                x
            
            | statement,_ -> 
                failwithf "Invalid statement: %A" statement

        recurse (statement, Array source)