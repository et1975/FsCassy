(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../build_output"

(**
Namespaces
========================

FsCassy is organized into 3 APIs:

- Statement API - compositional DSL for formulating the statements (includes pretty-printer) 
- Cassandra API - modules for working specifically with Cassandra (cluster connections, the interperter, etc)
- InMem API - modules for unit-testing the code that uses Statement API
  (note that not all Cassandra options are covered by in-mem implementation)


Statements
========================

The statements while composed are detached from implementation that will execute them, 
but are fully type-checking everything that goes into them (except for `PreparedStatement`). 
The Statement API directly reflects Cassandra operations, here are some of the examples:
*)

#r "FsCassy.dll"
open FsCassy

type SomeMappedTable = {x:int}
let exampleWhere : Statement<SomeMappedTable,_> =
        table
    >>= where (Quote.X(fun x -> x.x = 0)) 
    >>= read

let exampleTake : Statement<SomeMappedTable,_> =
        table
    >>= take 1
    >>= read

let exampleCount : Statement<SomeMappedTable,_> =
        table
    >>= withConsistency Consistency.Any
    >>= count

let exampleUpdate : Statement<SomeMappedTable,_> =
        table
    >>= where (Quote.X(fun x -> x.x = 0))
    >>= select (Quote.X(fun x -> { x with x = 10}))
    >>= update
    >>= withConsistency Consistency.Any
    >>= execute

let exampleUpdateIf : Statement<SomeMappedTable,_> =
        table
    >>= where (Quote.X(fun x -> x.x = 0))
    >>= select (Quote.X(fun x -> { x with x = 10}))
    >>= updateIf (Quote.X(fun x -> x.x = 0))
    >>= withConsistency Consistency.Any
    >>= execute

let exampleDelete : Statement<SomeMappedTable,_> =
        table
    >>= where (Quote.X(fun x -> x.x = 0))
    >>= delete
    >>= execute

let exampleUpsert : Statement<SomeMappedTable,_> =
        table
    >>= upsert {x = 100}
    >>= withConsistency Consistency.Any
    >>= execute
        

(**
Prepared statements
========================
Prepared statements are a fallback mechanism: some of the CQL features are not implemented (or not working as inteded) via the .NET driver.
See Cassandra docs for more information.

For most operations you will be able to use the provided DSL.
*)

(**
Interpreters
========================

Interpreters parse statements and carry out the actual work, following functions are available:

- Cassandra interpreter
  Cassandra interpreter uses [Table API](http://datastax.github.io/csharp-driver/features/components/linq/) 
  and relies on the same type mapping facilities.

- Pretty printer
  Humand-readable rough approximmation of the statement and it's arguments.

- In-Mem interpreter
  Executes statements against a predefined list.


The library also defines an interface with an equivalent function, see the relevant discussion 
[here](http://stackoverflow.com/questions/42598677/what-is-the-best-way-to-pass-generic-function-that-resolves-to-multiple-types).

You don't have to use this interface, you can define your own or even make do without one using SRTP. 
If you do please consider contributing, we'd love to see what you come up with.

*)

(**
Cassandra driver and FsCassy API
========================

FsCassy exposes a small set of functions that make it easier to construct the relevant parts, thier use is completely optional:
- Cluster connectivity
- Session construction and parametrization
- LINQ Table construction

The purpose of these (reference) implementations is to construct the arguments for the interpreter. 
You may want to have your own implementations instead using the official Cassandra API.
*)

(**
InMem API
========================

InMem implmenetation of the interpreter takes a ResizeArray as the source to work agains and can be supplied instead
of Cassandra interpreter:
*)

let xs = [ {x = 1}
           {x = 2}
           {x = 3}
           {x = 4}
           {x = 0}
           {x = 5}
           {x = 0}
           {x = 6} ]
let ``InMem reads``() = 
    table >>= read 
    |> InMem.Interpreter.execute (ResizeArray xs) 
    |> Async.RunSynchronously
