<!-- -*-visual-line-*- -->

[[Previous]](ziggy-gui.md)
[[Up]](ziggy-gui.md)
[[Next]](instances-panel.md)

## Starting a Pipeline

Let's do this.

### Get to the Pipelines Panel

The left-hand column is a *content menu* for the application. Select the `Pipelines` item and you should see this:

<img src="images/pipelines-panel.png" style="width:32cm;"/>

The table lists all the pipelines that are defined on this cluster, which in this case is just one, the sample pipeline.

Before we move on, there is a feature of the table headers that's worth pointing out. You can resize the columns by dragging the separators between the column names in the header. Double-clicking on a separator will shrink-wrap or enlarge the column to fit the data. Finally, columns can be hidden or exposed by using the context menu in the table header and checking or unchecking the boxes next to the column names in the popup menu. These features will come in handy later.

Moving on, select the sample pipeline since it's the only one we've got, and press the `Start` button.

### Start the Pipeline

A new dialog box will pop up that looks like this:

<img src="images/start-pipeline.png" style="width:9cm;"/>

A lot of options! For now, just put some kind of identifying text in the `Pipeline instance name` text box and press the `Start` button.

### Monitor Progress

As soon as the dialog box disappears, select the `Instances` content menu item. The left side should look something like this<sup>1</sup>:

<img src="images/instances-running.png" style="width:18cm;"/>

Select your pipeline instance in the table. On the right you see this:

<img src="images/permuter-tasks.png" style="width:16cm;"/>

Notice a few things:

1. In the upper-right corner (not shown in the screen shot), the grey lights for `Pi` (pipeline) and `W` (workers) have turned green. These 4 lights are known as Ziggy's "stoplights" (or somewhat more derisively, "idiot lights"). The first green light means that a pipeline is running; the second one means that Ziggy has one or more running workers. For more information, take a look at the article on the [Monitoring Tab](monitoring.md).
2. The left-hand table is now populated. It shows one entry, with ID 1. This is known as the `Instances` table.
3. There are 2 right-hand tables that are populated as well.
   1. The upper table is the "scoreboard." It summarizes the task and subtask counts for each pipeline module.
   2. The lower table is the "tasks table." It shows that data receipt already ran to completion, and that there are 2 permuter tasks that are running in series.

As you watch, time progresses in the last column of the task table, while the Status values progress through the processing steps until the task is `COMPLETE`. The Subtasks column shows the number of completed subtasks over the total subtasks. If any of the subtasks encounter an error, the number of subtasks with errors will appear in parenthesis and the row will turn red. The Subtasks counts should change from `0/4` (zero out of four subtasks complete) to `4/4` (four out of four subtasks complete). The Subtasks counts in the scoreboard change in unison with the ones in the task table. Eventually the two permuter tasks will finish and the entry in the Status column will turn to `COMPLETE`. Then two new tasks appear:

<img src="images/flip-tasks.png" style="width:16cm;"/>

Finally, after a few more seconds, two new tasks, named `averaging`, appear. Shortly after that, we get to this state:

<img src="images/tasks-done.png" style="width:16cm;"/>

The pipeline and worker lights are grey again, the instance and all the tasks show `COMPLETE`. Congratulations! You've just run your first Ziggy pipeline!

At this point, you'd probably like an explanation of just what everything on the `Instances` panel is trying to tell you. If so, read on! Specifically, the article on [The Instances Panel](instances-panel.md).

There is one last thing to mention before you go. Once a pipeline has been run, it is `Locked`. At this point, any changes you make will result in a new version number and the new version will be unlocked (see [The Edit Pipeline Dialog Box](edit-pipeline.md)). If a pipeline is `Locked`, or the version is greater than zero, the `Rename` and `Delete` commands in the context menu for the sample pipeline will be disabled in the interest of data accountability (see [Data Accountability](data-accountability.md)).

<sup>1</sup> The Event name column is initially hidden to save space. See the article [Event Handler Examples](event-handler-examples.md) for information on how to show and use this column.

[[Previous]](ziggy-gui.md)
[[Up]](ziggy-gui.md)
[[Next]](instances-panel.md)
