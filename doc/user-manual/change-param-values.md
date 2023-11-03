<!-- -*-visual-line-*- -->

[[Previous]](start-end-nodes.md)
[[Up]](ziggy-gui.md)
[[Next]](organizing-tables.md)

## Changing Module Parameter Values

Module parameters can be changed to affect how the algorithm behaves. To see how this works, select the `Parameter Library` content menu item. You'll see this:

<img src="images/parameter-library.png" style="width:32cm;"/>

As you can see, all of the parameter sets defined in `pl-sample.xml` are represented here. What else does the table tell us?

- The `Type` column is the name of the Java class that supports the module parameter set. For now you can ignore this.
- The `Version` column shows the current version of the parameter set. They all show zero because none of the parameter sets has been modified since they were imported from `pl-sample.xml`.
- The `Locked` column shows whether the current version of each parameter set is locked. What that means is this: before a version of a parameter set is used in the pipeline, it's unlocked, and the user can make changes to it; once the version has been used in processing, that version becomes locked, and any changes the user makes will create a new version (that is unused, hence unlocked). The versioning and locking features allow Ziggy to preserve a permanent record of the parameters used in each instance of each pipeline.

Now: double-click the Algorithm Parameters row in the table. You'll get a new dialog box:

<img src="images/edit-param-set.png" style="width: 11cm;"/>

The parameters that were defined as booleans in `pl-sample.xml` have check boxes you can check or uncheck. The other parameter types mostly behave the way you expect, but the array types offer some additional capabilities. If you click the `dummy array parameter` parameter, it will change thusly:

<img src="images/edit-array-1.png" style="width:11cm;"/>

If you click the "X", all the values will be deleted, which is rarely what you want. Instead click the other button. You'll get this window:

<img src="images/edit-array-2.png" style="width:9cm;"/>

This allows you to edit the array elements, remove them, add elements, etc., in a more GUI-natural way. Go ahead and change the second element (`Element` 1 reflecting the zero-based nature of Java arrays) to 4 from 2. Press the `OK` button and then press the `Save` button on the Edit parameter set dialog. The `Version` for Algorithm Parameters will now be set to 1, and the `Locked` checkbox is unchecked.

If you were to now run the sample pipeline, when you returned to the parameter library window, version 1 of `Algorithm Parameters` will show as locked.

[[Previous]](start-end-nodes.md)
[[Up]](ziggy-gui.md)
[[Next]](organizing-tables.md)
