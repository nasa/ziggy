<!-- -*-visual-line-*- -->

[[Previous]](instances-panel.md)
[[Up]](ziggy-gui.md)
[[Next]](change-param-values.md)

## Changing the Start and End Nodes

As you perform your data processing activities, it won't always be necessary to run the whole, entire pipeline. As a simple example: any time you're reprocessing, you won't need to re-run the data receipt step in any pipelines that start with data receipt.

Fortunately, Ziggy provides tools that allow you to manage these conditions.

To see how to do this, return to the `Pipelines` panel, select the `sample` pipeline, and press the `Start` Button. This time, though, when the Start pipeline dialog box appears, don't hit the `Start` button on the dialog box just yet. Instead, click the `Override start` and `Override stop` check boxes. This will allow you to use the pull-down menus to select a different start and end node. For example:

<img src="images/start-end-nodes.png" style="width: 9cm;/"/>

Now press the `Start` button and return to the `Instances` panel. The pipeline will be running instance 2, which will start with the `permuter` tasks. Eventually the panel will look like this:

<img src="images/gui-start-end-adjusted.png" style="width: 35cm;/"/>

As advertised, the pipeline ran again but this time it skipped data receipt and stopped short of the averaging tasks.

[[Previous]](instances-panel.md)
[[Up]](ziggy-gui.md)
[[Next]](change-param-values.md)
