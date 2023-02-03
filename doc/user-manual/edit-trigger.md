<!-- -*-visual-line-*- -->

[[Previous]](parameter-overrides.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)

## The Edit Trigger Dialog Box

The Edit Trigger dialog box is a kind of grab-bag of various features related to managing pipeline execution. The name of the dialog box relates to an archaic time in which there were specialized Java objects, called "triggers," for launching pipelines (this is also why starting a pipeline requires the user to hit a button labeled "Fire!" Because you fire a trigger).

To get to this dialog box, go to Operations > Triggers, and double-click the pipeline you're interested in. You'll get this dialog box:

<img src="images/edit-trigger.png" style="zoom:50%;" />

What does all this stuff do? Let's go through it from the top to the bottom ("Hmm -- I got 'em!'" -C+C Music Factory).

### Trigger Panel

The main actions you can take from this panel are viewing or exporting parameters, and validation of the pipeline. 

The `export params` button exports the parameters used by this pipeline, in a kind of hokey and non-standard format. The advantage this has over using the export function on the Parameters Configuration panel is that it only exports the parameter sets used by this pipeline. In a situation where you have a lot of pipelines, with a lot of parameters, it's potentially useful to be able to see just the parameters for a given pipeline. 

The `report` button brings up a new window that shows the modules and parameter sets for this pipeline. The report can also be saved to a text file. Again: mainly useful in the context of a fairly complex system, in which you want to isolate the bits and pieces of a specified pipeline from the general mass of bits and pieces in the system. 

The `validate` button is a vestige of a bygone era. It looks for issues with the pipeline's parameter sets that are largely impossible today, but which were common in Ziggy's predecessor software packages. 

### Priority Panel

We've discussed in the past the fact that Ziggy sometimes faces a situation in which Ziggy has more tasks waiting for attention than it has worker threads ready to service the tasks. In this case, Ziggy has to prioritize the tasks to ensure that the most critical ones get attention first. 

The pipeline priority is one way this sorting occurs. Ziggy uses a system in which priority == 0 is the highest priority, priority == 4 the lowest. Tasks with higher priority get to leap ahead of tasks with lower priority in the queue. 

So how to tasks get assigned a priority? 

All tasks that are running for the first time get assigned a priority equal to the priority of the parent pipeline. In this example, the sample pipeline has a priority of 4, meaning that all tasks for this pipeline will have the lowest possible priority on their first pass through the system. All tasks that are being rerun or restarted are given priority 0, so all restart / rerun tasks have priority over any never-yet-run tasks. 

All pipelines, in turn, are initially created with priority 4, meaning that all pipelines will, by default, produce tasks at priority 4. Thus, all tasks from all pipelines compete for workers with a "level playing field," if you will. Usually this is the situation that most users want. 

One case where this isn't true is missions that have occasional need for much faster turnaround of data processing. That is to say, most data can be processed through Pipeline X on a first-come, first-served basis; but occasionally there will be a need to process a small amount of just-acquired data through Pipeline Y immediately. To ensure that this happens, you can set Pipeline Y to have a priority of 0.

### Pipeline Parameter Sets Panel

Say that five times fast. 

Anyway.

This panel shows a list of the parameter sets that are assigned at the pipeline level (that is to say, parameter sets that are made available to every task regardless of which processing module it uses). The `add` button allows you to select any parameter set in the parameter library and make it a pipeline parameter set for this pipeline. The `edit values` button allows you to change the values of the parameters in a given set. The `select` and `auto-assign` buttons do things that used to be useful, but in the current version of Ziggy are not. 

Given that the Parameter Library Configuration panel already allows you to view and edit parameters, why is it useful to have this panel on this dialog box? Again -- in the case where you have a lot of pipelines and a lot of parameters, it's useful to be able to view the parameters for a given pipeline in isolation. This allows you to avoid confusion about which parameters go to which pipelines. 

### Modules Panel

The `Modules` panel offers functions that address the pipeline modules within a given pipeline. 

The display shows the modules in the pipeline, sorted in execution order. You can select a module and then use the buttons beneath the list to execute hopefully-useful actions:

#### Task Information Button

This button produces a table of the tasks that Ziggy will produce for the specified module if you start the pipeline. This takes into account whether the module is configured for "keep-up" processing or reprocessing, the setting of the taskDirectoryRegex string (which allows the user to specify that only subsets of the datastore should be run through the pipeline). For each task, the task's unit of work description and number of subtasks are shown. 

#### Edit Parameters Button

This button brings up a table of module-level parameter sets. The user can edit the existing parameter sets, or add a parameter set to a given module. The not-useful `select` and `auto-assign` buttons are also present. 

#### Re-Sync Button 

This button deletes any local changes you've made to the parameter values and/or parameter set assignments, and reloads the parameters for the selected module from the database. 

#### Remote Execution Button

This button brings up the remote execution dialog box. See [the article on the remote execution dialog](remote-dialog.md) for more information. 

[[Previous]](parameter-overrides.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)
