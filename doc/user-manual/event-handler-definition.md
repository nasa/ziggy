<!-- -*-visual-line-*- -->

[[Previous]](event-handler-intro.md)
[[Up]](event-handler.md)
[[Next]](event-handler-examples.md)

## Event Handler: Defining Event Handlers

Defining an event handler is surprisingly easy. At least, it's surprising to me. Maybe it will be to you as well.

### The Event Handler XML File

You probably already guessed that it would be an XML file. The event handler XML files, by convention, have names that start with `pe-`.

The event handler demonstrated in the sample pipeline is in [pe-sample.xml](../../sample-pipeline/config/pe-sample.xml). Leaving aside the boilerplate that starts and ends the XML file, here's the event definition in all its glory:

```xml
<pipelineEvent name="data-receipt" pipelineName="sample"
               enableOnClusterStart="false"
               directory="${data.receipt.dir}"/>
```

 The `pipelineEvent` element straightforwardly defines the name of the event handler itself and the name of the pipeilne it triggers. The `enableOnClusterStart` attribute tells Ziggy whether the event handler should be enabled immediately when you type `runjava cluster start`, or whether it should require a human to go in and turn it on via the console after the cluster is already up and running.

The `directory` attribute is, of course, the directory where the event handler looks for its ready files. In this case, the data receipt event handler looks for ready files in the same directory where the data receipt pipeline module looks for files (or directories of files). That was a design choice, though you wouldn't need to do it that way: you could have a totally separate directory for the event handler to watch, if such was your preference.

Note here that, rather than a normal string, the directory attribute can take a string that needs to be expanded into [one of Ziggy's properties](properties.md). This allows you to specify all the watched directories in the properties file. Note **that this is the only attribute or element in all of Ziggy's XML infrastructure that allows the use of property expansion notation!** That's strictly because it's the only place where we thought we needed it. If you need this added to some other part of the XML infrastructure for your purposes, let us know. We can make it happen!

<!--

TODO Consider allowing property interpolation more generally (ZIGGY-173: Add property interpolation to XML files) and say so here. In addition, it would also be good to talk about properties in XML files in the "Write the Pipeline Configuration Files" section of configuring-pipeline.md and to mention that you have to restart the cluster (which we haven't done yet, but will soon, we promise!)

-->

[[Previous]](event-handler-intro.md)
[[Up]](event-handler.md)
[[Next]](event-handler-examples.md)

