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
config --configType TYPE [--instance ID | --pipeline NAME]
                       Display pipeline configuration
display [[--displayType TYPE] --instance ID | --task ID]
                       Display pipeline activity
halt [--instance ID | --task ID ...]
                       Halts the given task(s) or all incomplete tasks in the given instance
log --task ID | --errors
                       Request logs for the given task(s)
restart [--restartMode MODE] [--instance ID | --task ID ...]
                       Restarts the given task(s) or all halted tasks in the given instance
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
 -r,--restartMode <arg>   Restart mode (restart-from-beginning, resume-current-step, resubmit,
                          resume-monitoring; default: restart-from-beginning)
 -t,--task <arg>          Comma-separated list of task IDs and ranges
```

### Commands

We'll cover each command in turn.

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

**halt --instance ID | --task ID ...**

Halt the given task(s) or all incomplete tasks in the given instance. Multiple `--task options` may be given. Task options can be comma-separated lists of IDs and ranges.

```console
$ ziggy console halt --task 2 --task 3,4,7-10
```

**log --task ID | --errors**

Request logs for the given task(s). This command is not yet implemented.

**restart [--restartMode MODE] --instance ID | --task ID ...**

Restart the given task(s) or all halted tasks or failed transitions in the given instance. Multiple `--task options` may be given. Task options can be comma-separated lists of IDs and ranges. Tasks are started with the given restart mode. The default is to restart from the beginning of the pipeline module. This command only has effect on tasks that errored out or with instances with failed transitions.

```console
$ ziggy console restart --restartMode resubmit --task 2 --task 3,4,7-10
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
