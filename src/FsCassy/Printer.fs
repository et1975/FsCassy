namespace FsCassy

/// Pretty printer
module Printer =
    let internal formatClause : Clause<'t,_> -> _ =
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
