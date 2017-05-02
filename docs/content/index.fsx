(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../build_output"

(**
Functional F# API for Cassandra
======================
FsCassy offers several improvements for F# expereience over the .NET driver: 
F# quotations support for statically-typed update queries, a composable DSL for statements and 
Cassandra and an InMem statement interpreters. The pluggbale interpreter extends the reach of unit-testing 
into the code that would not be otherwise testable in isolation. 


Installing
======================

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The FsCassy library can be <a href="https://nuget.org/packages/FsCassy">installed from NuGet</a>:
      <pre>PM> Install-Package FsCassy</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Example
-------

This example demonstrates the functional language defined by FsCassy:

*)
#r "FsCassy.dll"
#r "Cassandra.dll"
open FsCassy

// Some table we have mapped to a type
type SomeMappedTableType = {x:int}

// a composed statement, if pass it to an interpreter, we'll get Async<unit> back
let exampleUpdateIf : Statement<SomeMappedTableType,_> =
        table
    >>= where (Quote.X(fun x -> x.x = 0))
    >>= select (Quote.X(fun x -> { x with x = 10}))
    >>= updateIf (Quote.X(fun x -> x.x = 0))
    >>= withConsistency Consistency.Quorum
    >>= execute


(**

Samples & documentation
-----------------------


 * [Tutorial](tutorial.html) goes into more details.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------
The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. 

The library is available under Apache license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/Prolucid/FsCassy/tree/master/docs/content
  [gh]: https://github.com/Prolucid/FsCassy
  [issues]: https://github.com/Prolucid/FsCassy/issues
  [readme]: https://github.com/Prolucid/FsCassy/blob/master/README.md
  [license]: https://github.com/Prolucid/FsCassy/blob/master/LICENSE.md


Copyright 2017 Prolucid Technologies Inc
*)
