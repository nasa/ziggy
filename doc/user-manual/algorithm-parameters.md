<!-- -*-visual-line-*- -->

[[Previous]](other-pipeline-algorithms.md)
[[Up]](configuring-pipeline.md)
[[Next]](datastore.md)

## Algorithm Parameters

### What Are Algorithm Parameters, Anyway?

Algorithm parameters are a form of data used to control the pipeline: either the way that Ziggy executes the processing, or the way that the algorithms themselves function. For example, algorithm parameters can tell an algorithm to enable or disable a particular processing step; set convergence criteria for model fitting algorithms; set the number of data points to use in a median filter; or, really, anything of that nature.

Algorithm parameters are organized into groups known as "parameter sets." When Ziggy provides parameters to a pipeline node, it's in the form of one or more parameter sets, rather than individual parameters. There's no limit on the number of parameter sets that can be provided to a pipeline or a pipeline node, so don't feel that you need to cram a hundred parameters into one parameter set; it's probably the case that the parameters can be logically grouped into sets, and you should feel free to group them that way and then pass all the needed parameter sets to the nodes that need them.

Because the sample pipeline is extremely simple, we have only one parameter set, in `sample-pipeline.xml`:

```xml
<parameterSet name="Algorithm Parameters">
  <parameter name="throw exception subtask 0" value="false" type="boolean"/>
  <parameter name="produce output subtask 1" value="true" type="boolean"/>
  <parameter name="dummy array parameter" value="1, 2, 3" type="intarray"/>
  <parameter name="execution pause seconds" value="5" type="int"/>
</parameterSet>
```

Each parameter set must have a unique name, in this case "Algorithm Parameters". Each parameter must have a name that is unique within the parameter set (i.e., I could have another parameter named "dummy array parameter" in a different parameter set, but obviously I can't have 2 parameters with the same name in the same set).

Each individual parameter in a set has a name, a value, and a type. The allowed types the usual ones you would expect:
- `boolean`: either `true` or `false`
- `byte`: an integer from -128 to +127
- `short`: an integer value from -32768 to +32767
- `int`: a 32-bit signed integer value
- `long`: a 64-bit signed integer value
- `float`: a floating-point value represented internally as a 32-bit IEEE floating point number
- `double`: a floating-point value represented internally as a 64-bit IEEE floating point number
- `String`: a string of Unicode characters

The parameters can be scalar or they can be arrays: to specify an array, append `array` after the type name, as shown by the `dummy array parameter`.

Note that both the parameter set name and the parameter name can have whitespace in them. This is
for convenience when showing them in the Ziggy pipeline console UI application. However, when the
parameter sets are provided to the algorithm, as shown in the article on
[Configuring a Pipeline](configuring-pipeline.md), any spaces in those names are converted
to underscores ("_").

We recommend you *do not use* spaces in the parameter names, nor in the parameter set names, to avoid
confusion. However, we allow whitespace, and we used it in the sample pipeline to show both
that is is possible and that the algorithm code must use underscores instead.

<!--
TODO Tell what happens if you use other non-ASCII characters in names. Since there is no restriction
in the schema (such as making them IDs), any unicode characters could be used in names in the XML
definitions. What do they get translated to?
-->

[[Previous]](other-pipeline-algorithms.md)
[[Up]](configuring-pipeline.md)
[[Next]](datastore.md)
