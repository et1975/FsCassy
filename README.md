FsCassy [![Windows Build](https://ci.appveyor.com/api/projects/status/8v3ox0ha8im7dgnt?svg=true)](https://ci.appveyor.com/project/et1975/FsCassy) [![Mono/OSX build](https://travis-ci.org/Prolucid/FsCassy.svg?branch=master)](https://travis-ci.org/Prolucid/FsCassy) [![NuGet version](https://badge.fury.io/nu/FsCassy.svg)](https://badge.fury.io/nu/FsCassy)
=======

FsCassy offers several improvements over the official .NET driver:
 
- F# quotations support for statically-typed update queries, 
- a composable DSL for statements,
- a Pretty printer, Cassandra and InMem statement interpreters

The pluggable interpreter extends the reach of unit-testing into the code that would not otherwise be testable in isolation. 

=======

Some assembly required: please see the tests for examples of the interpreter construction.