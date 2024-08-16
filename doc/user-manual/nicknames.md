<!-- -*-visual-line-*- -->

[[Previous]](edit-pipeline.md)
[[Up]](dusty-corners.md)
[[Next]](advanced-uow.md)

## Creating Ziggy Nicknames

Ziggy nicknames were introduced in the article [Running the Pipeline](running-pipeline.md). Those nicknames are defined by properties in `ziggy.properties` with `ziggy.nickname.` prefixes. There is another property called `ziggy.default.jvm.args` that is added to any JVM arguments that appear in those properties.

You can add your own nicknames to your own property file that is referred to by `PIPELINE_CONFIG_PATH`. You can find examples of the format in `ziggy.properties`, which is this:

```
ziggy.nickname.<nickname> = <fully qualified class name>|<logfile basename>|<space-delimited JVM args>|<space-delimited program args>
```

The pieces of this entry are as follows:

| Item | Description |
| ---- | ----------- |
| nickname | The nickname you'd like to use such as `ziggy console`. |
| fully qualified class name | The classname for the console is `gov.nasa.ziggy.ui.ZiggyConsole`. |
| logfile basename | This item for the console is, namely, console, which results in a logfile in `${ziggy.pipeline.results.dir}/logs/cli/console.log`. If this is left blank, then the log filename will be `ziggy.log`. |
| JVM arguments | If you need to raise the default heap size, this is the place to do it. |
| program arguments | These arguments will always be passed on to the program in addition to any you pass on the `ziggy` command line. |

Your nickname's entry may make use of other properties in your property file. Refer to that property like this: `${property}`.

For more information, try `perldoc ziggy`.

Once you've added some nicknames, you can create a symbolic link to the ziggy program and make it your own. For example, in the Science Processing Operations Center (SPOC) in TESS, we linked spocops to ziggy so that we could run `spocops import-target-list`.

[[Previous]](edit-pipeline.md)
[[Up]](dusty-corners.md)
[[Next]](advanced-uow.md)
