<!-- -*-visual-line-*- -->

[[Previous]](parameter-overrides.md)
[[Up]](dusty-corners.md)
[[Next]](nicknames.md)

## The Edit Pipeline Dialog Box

The Edit Pipeline dialog box is used to edit pipeline parameter sets and modules.

To get to this dialog box, open the pipelines panel and double-click the pipeline you're interested in. You'll get this dialog box:

<img src="images/edit-pipeline.png" style="width:13cm;" />

What does all this stuff do? Let's go through it from the top to the bottom ("Hmm -- I got 'em!'" -C+C Music Factory).

### Pipeline Section

The main actions you can take from this section is to validate the pipeline and view or export parameters.                   

The `Validate` button is a vestige of a bygone era. It looks for issues with the pipeline's parameter sets that are largely impossible today, but which were common in Ziggy's predecessor software packages.

The `Report` button brings up a new window that shows the modules and parameter sets for this pipeline. The report can also be saved to a text file. This dialog is mainly useful in the context of a fairly complex system, in which you want to isolate the bits and pieces of a specified pipeline from the general mass of bits and pieces in the system.

The `Export parameters` button exports the parameters used by this pipeline, in a kind of hokey and non-standard format. The advantage this has over using the export function on the parameter library panel is that it only exports the parameter sets used by this pipeline. In a situation where you have a lot of pipelines, with a lot of parameters, it's potentially useful to be able to see just the parameters for a given pipeline.

The `Priority` field takes a little more explanation. We've discussed in the past the fact that Ziggy sometimes faces a situation in which Ziggy has more tasks waiting for attention than it has worker processes ready to service the tasks. In this case, Ziggy has to prioritize the tasks to ensure that the most critical ones get attention first. The pipeline priority is one way this sorting occurs. Tasks with higher priority get to leap ahead of tasks with lower priority in the queue. The available priorities are LOWEST, LOW, NORMAL, HIGHEST, HIGH. 

So how to tasks get assigned a priority?

All tasks that are running for the first time get assigned a priority equal to the priority of the parent pipeline. In this example, the sample pipeline has a priority of NORMAL, meaning that all tasks for this pipeline will have the lowest possible priority on their first pass through the system. Tasks that are being persisted (which happens on a separate pass through the task management system) do so with priority HIGH, so persisting results takes precedence over starting new tasks. Tasks that are being rerun or restarted do so with priority HIGHEST, which means exactly what it sounds like. 

All pipelines, in turn, are initially created with priority NORMAL, meaning that all pipelines will, by default, produce tasks at priority NORMAL. Thus, all tasks from all pipelines compete for workers with a "level playing field," if you will. Usually this is the situation that most users want.

One case where this isn't true is missions that have occasional need for much faster turnaround of data processing. That is to say, most data can be processed through Pipeline X on a first-come, first-served basis; but occasionally there will be a need to process a small amount of just-acquired data through Pipeline Y immediately. To ensure that this happens, you can set Pipeline Y to have a priority of HIGH or HIGHEST.

Finally, the read-only `Valid?` checkbox is ticked after the `Validate` button is pressed, presuming all went well.  

### Pipeline Parameter Sets Section

Say that five times fast.

Anyway.

This section shows a list of the parameter sets that are assigned at the pipeline level (that is to say, parameter sets that are made available to every task regardless of which processing module it uses). The `Add` button allows you to select any parameter set in the parameter library and make it a pipeline parameter set for this pipeline. The `Edit` button allows you to change the values of the parameters in a given set. The `Select` and `Auto-assign` buttons do things that used to be useful, but in the current version of Ziggy are not.

Given that the parameter library panel already allows you to view and edit parameters, why is it useful to have this section on this dialog box? Again -- in the case where you have a lot of pipelines and a lot of parameters, it's useful to be able to view the parameters for a given pipeline in isolation. This allows you to avoid confusion about which parameters go to which pipelines.

### Modules Section

The `Modules` section offers functions that address the pipeline modules within a given pipeline.

The display shows the modules in the pipeline, sorted in execution order. You can select a module and press one of the following buttons:

#### Task Information Button

This button produces a table of the tasks that Ziggy will produce for the specified module if you start the pipeline. This takes into account whether the module is configured for "keep-up" processing or reprocessing, the setting of the taskDirectoryRegex string (which allows the user to specify that only subsets of the datastore should be run through the pipeline). For each task, the task's unit of work description and number of subtasks are shown. If the table is empty, it means that the relevant files in the datastore are missing. The datastore is populated by [Data Receipt](data-receipt.md); that article will help you ingest your data into the datastore so that the task information table can calculate the number of tasks and subtasks the input data will generate.

#### Resources Button

If you look back at [the article on running the cluster](running-pipeline.md), you'll note that we promised that there was a way to set a different limit on the number of workers for each pipeline module. This button is that way! 

More specifically, if you press the `Resources` button, you'll get the `Worker resources` dialog box that displays a table of the modules and the current max workers and heap size settings. To change these settings from the default, either double-click on a module or use the context menu and choose the `Edit` command. This brings up the `Edit worker resources` dialog box where you can uncheck the Default checkboxes and enter new values for the number of workers or the heap size for that module. Note that the console won't let you enter more workers than cores on your machine, which is found in the the tooltip for this field. Henceforth, Ziggy will use those values when deciding on the maximum number of workers to spin up for that module and how much memory each should be given. Typically, as you increase the number of workers on a single host, you'll need to reduce the amount of heap space for each worker so that the total memory will fit within the memory available on the machine.

Alternately, you may want to do the reverse: take a module that has user-set maximum workers or heap size values and tick the Default checkboxes to go back to using the defaults.

#### Parameters Button

This button brings up the `Edit parameter sets` dialog box that displays a table of module-level parameter sets. The user can edit the existing parameter sets, or add a parameter set to a given module. The not-useful `Select` and `Auto-assign` buttons are also present.

#### Remote Execution Button

This button brings up the `Edit remote execution parameters` dialog box. See [the article on the remote execution dialog](remote-dialog.md) for more information.

### Save and Cancel

You're probably thinking, "How much do I really need to know about Save and Cancel buttons?" Well, yeah, they're mostly self-explanatory. But not completely!

To save changes that you've made on the `Edit pipeline` dialog box, or any other dialog box that it spawned, use the `Save` button. To discard all your changes, use the `Cancel` button. 

The points I'm trying to make here are twofold:

1. Anything you do after you launch the `Edit pipeline` dialog box can be discarded, and will only be preserved when you press `Save`.
2. The `Save` and `Cancel` buttons on the `Edit pipeline` dialog box also apply to changes made on the `Edit remote execution parameters` dialog box, the `Edit parameter sets` dialog box, etc.

[[Previous]](parameter-overrides.md)
[[Up]](dusty-corners.md)
[[Next]](nicknames.md)
