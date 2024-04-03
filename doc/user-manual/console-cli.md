<!-- -*-visual-line-*- -->

[[Previous]](event-handler-labels.md)
[[Up]](user-manual.md)
[[Next]](dusty-corners.md)

## The Console Command-line Interface (CLI)

The command-line interface for the console contains enough functionality to start, stop, and view pipelines. Let's start by displaying the help for the console.

```console
$ ziggy console --help
usage: ZiggyConsole command [options]

Commands:
cancel                 Cancel running pipelines
config --configType TYPE [--instance ID | --pipeline NAME]
                       Display pipeline configuration
display [[--displayType TYPE] --instance ID | --task ID]
                       Display pipeline activity
log --task ID | --errors
                       Request logs for the given task(s)
reset --resetType TYPE --instance ID
                       Put tasks in the ERROR state so they can be restarted
restart --task ID ...  Restart tasks
start PIPELINE [NAME [START_NODE [STOP_NODE]]]
                       Start the given pipeline and assign its name to NAME
                       (default: NAME is the current time, and the NODES are
                       the first and last nodes of the pipeline respectively)
version                Display the version (as a Git tag)

Options:
 -c,--configType <arg>    Configuration type (data-model-registry | instance | pipeline |
                          pipeline-nodes)
 -d,--displayType <arg>   Display type (alerts | errors | full | statistics | statistics-detailed)
 -e,--errors              Selects all failed tasks
 -h,--help                Show this help
 -i,--instance <arg>      Instance ID
 -p,--pipeline <arg>      Pipeline name
 -r,--resetType <arg>     Reset type (all | submitted)
 -t,--task <arg>          Task ID
```

### Commands

We'll cover each command in turn.

**cancel**

This command is currently broken. It will be renamed to halt and given the same semantics as the halt commands in the GUI in a future version.

**config --configType TYPE [--instance ID | --pipeline NAME]**

Display pipeline configuration. The four configuration types that can be displayed are `data-model-registry`, `instance`, `pipeline`,  and `pipeline-nodes`. The `data-model-registry` type displays the content of the known models. The `instance` type displays details for all of the pipeline instances, including parameter sets and module definitions. Use the `--instance` option to limit the display to the given instance. The `pipeline` type displays details for all of the pipeline definitions, including parameter sets and module definitions. Use the `--pipeline` option to limit the display to the given pipeline. Finally, the `pipeline-nodes` type displays a short list of the nodes for the pipeline named with the `--pipeline` option.

```console
$ ziggy console config --configType pipeline --pipeline sample
```

**display [[--displayType TYPE] --instance ID | --task ID]**

Display pipeline activity. When the command appears by itself, a table of instances is shown. If an instance ID is provided, then instance and task summaries are shown. Use the `displayType` option to increase the level of detail or to show other elements. The `alerts` and `errors` types will show those elements associated with the given instance respectively. The `full` option adds a table of the tasks. The `statistics` option shows timing information for each task. The `statistics-detailed` option is similar, but a PDF is generated.

```console
$ while true; do ziggy console display --instance 2 --displayType full; sleep 15; done
```

**log --task ID | --errors**

Request logs for the given task(s). This command is not yet implemented.

**reset --resetType TYPE --instance ID**

This command is currently broken. It will be renamed to halt and given the same semantics as the halt commands in the GUI in a future version.

**restart restart --task ID ...**

Restart tasks. Multiple `--task options` may be given. Tasks are started from the beginning. This command only has effect on tasks in the ERROR state.

```console
$ ziggy console restart --task 2 --task 3
```

**start PIPELINE [NAME [START_NODE [STOP_NODE]]]**

Start the given PIPELINE and assign its name to NAME. If NAME is omitted, the pipeline will be named with the current time. The start and stop nodes can be provided, but if they are not, the first and last nodes of the pipeline are used instead.

```console
$ ziggy console start sample "Test 1"
$ ziggy console start sample "Test 2" permuter flip
```

**version**

Display the version (as a Git tag).


[[Previous]](event-handler-labels.md)
[[Up]](user-manual.md)
[[Next]](dusty-corners.md)
