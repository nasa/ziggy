<!-- -*-visual-line-*- -->

[[Previous]](redefine-pipeline.md)
[[Up]](dusty-corners.md)
[[Next]]

## Parameter Overrides

At this point, we've learned the following about how to define and/or redefine parameters:

- The initial parameter values are loaded into the database during pipeline initialization (see [the article on running the cluster](running-pipeline.md) for a refresher on this).
- Parameter values can be changed using the parameter set editor dialog box (see [the article on changing parameter values](change-param-values.md) for a refresher on this).
- Parameters can also be imported using the Import button on the parameters Configuration item (see [the article on parameter's dusty corners](more-parameters.md) for a refresher on this).

So far, so good. But consider the following scenario:

Imagine that you have lots of parameter sets, with lots and lots of parameters, and for some reason you have a situation in which you want to change a handful of parameters while leaving all the others unchanged. How do you do that?

The obvious way is to export the parameter library using the `Export` button, edit the resulting file to incorporate your changes, and then import the library using the `Import` button. This works, but it's potentially a bit of a hassle. Worse yet, it results in a potentially very large XML file in which most of the parameters are unchanged from the database, while a few are changed. A user looking over the file months later would have no idea what was changed. Thus, it makes the process of configuration management more difficult. 

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