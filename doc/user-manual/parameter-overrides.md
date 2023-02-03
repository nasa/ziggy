<!-- -*-visual-line-*- -->

[[Previous]](more-parameter-sets.md)
[[Up]](dusty-corners.md)
[[Next]](redefine-pipeline.md)

## Parameter Overrides

At this point, we've learned the following about how to define and/or redefine parameters:

- The initial parameter values are loaded into the database during pipeline initialization (see [the article on running the cluster](running-pipeline.md) for a refresher on this).
- Parameter values can be changed using the parameter set editor dialog box (see [the article on changing parameter values](change-param-values.md) for a refresher on this).
- Parameters can also be imported using the Import button on the parameters Configuration item (see [the article on parameter's dusty corners](more-parameters.md) for a refresher on this).

So far, so good. But consider the following scenario:

Imagine that you have lots of parameter sets, with lots and lots of parameters, and for some reason you have a situation in which you want to change a handful of parameters while leaving all the others unchanged. How do you do that?

One way would be to [use the console to manually change the parameters](change-param-values.md). This is a bit cumbersome. It's especially cumbersome if you're changing the parameters to values that are specified by some other person or entity. In that case, they're writing down the changes they want on a Post-It or something, and you're going through implementing their changes (or worse, they just verbally tell you what they want and hope you remember everything they said). 

Another way is to export the parameter library using the `Export` button, edit the resulting file to incorporate the changes, and then import the library using the `Import` button. This is a better option, especially if you're incorporating changes that come from somebody else (in that case you can tell that other person to edit the file), but it's potentially a bit of a hassle. It also introduces some potential for screwups because whoever edits the file has to find the exact parameters, in the exact parameter sets, that need to be modified. If there are a lot of parameters, and not too many need to be changed, this proves to be onerous and error-prone. 

Fortunately, there's a tool that lets you get around this problem: you can write a parameter library XML file that specifies only the parameters you want to change. When it imports, the specified parameters will change and the rest remain the same. 

### The Parameter Override File

To see what's involved, take a look at `sample-pipeline/config-extra/pl-with-overrides.xml` . Here's the total contents of that file:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<parameterLibrary override-only="true">
    <parameter-set name="Algorithm Parameters">
        <parameter name="throw exception subtask_0" value="true" type="boolean"/>
    </parameter-set>
    <parameter-set name="Multiple subtask configuration" 
    classname="gov.nasa.ziggy.uow.TaskConfigurationParameters">
        <parameter name="reprocess" value="true"/>
    </parameter-set>
</parameterLibrary>
```

In this file, the `parameterLibrary` element has its `override-only` attribute set to `true`. Other than that, only two parameters are defined: the `throw exception subtask_0` parameter in `Algorithm Parameters`, and the `reprocess` parameter in `Multiple subtask configuration` . 

By specifying that override-only is true, Ziggy knows that the user doesn't want any other parameter in any parameter set to be altered. 

The resulting file can be imported in the usual ways: either by the `Import` button on the `Parameter Library` item in `Configuration`, or by using the `runjava` command `pl-import` . In either case, examining the parameter library via the console shows that the two selected parameters are changed, and the remainder are unchanged. 

[[Previous]](more-parameter-sets.md)
[[Up]](dusty-corners.md)
[[Next]](redefine-pipeline.md)