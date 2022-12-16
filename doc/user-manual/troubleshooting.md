[[Previous]](properties.md)
[[Up]](user-manual.md)
[[Next]](log-files.md)

## Troubleshooting Pipeline Execution

It's been said that the only things in life that are guaranteed are death and taxes, but software failures should probably be on that list as well. When you start setting up your own pipelines, sure as sugar you're going to encounter problems! Here's where we get started helping you figure out where it went wrong, why it went wrong, and how to get back on track.

### Creating an "Exception" in the Sample Pipeline

On the console, go back to the `Configuration` tab, select `Parameter Library`, and double-click `Algorithm Parameters`. You'll see that there's a boolean parameter, `throw exception subtask_0`. Check the check box and save the parameter set.

What this does is to tell the permuter's Python-side "glue" code that it should deliberately throw an exception during the processing of subtask zero. Obviously you shouldn't put such a parameter into your own pipeline! But in this case it's useful because it allows us to generate an exception in a controlled manner and watch what happens.

Now go to the `Operations` tab, to its Triggers tab, and set up to run `permuter` and `flip` nodes in the sample pipeline. Launch the pipeline and return to the `Instances` tab. After a few seconds you'll see something like this:

![](images/exception-1.png)

What you see is that at the moment when I took my screen shot, 2 subtasks had completed and 1 had failed in each task, which is why each has a `P-state` that includes `(4 / 2 / 1)` (i.e., total subtasks, completed subtasks, failed subtasks). After a little more time has passed, you'll see this:

![](images/exception-2.png)

What indications do we have that all is not well? Let me recount the signs and portents:

- The instance state is `ERRORS_STALLED`, which means that Ziggy can't move on to the next pipeline node due to errors.
- The task state is `ERROR`.
- The task P-states are `Ac` (algorithm complete), and subtask counts are 4 / 3 / 1, so 1 failed subtask per task, as expected.
- Both the `Pi` stoplight (pipelines) and the `A` stoplight (alerts) are red.

### [Log Files](log-files.md)

The first stop for understanding what went wrong is the log files. Ziggy produces a lot of these!

### [Using the Ziggy GUI for Troubleshooting](ziggy-gui-troubleshootihng.md)

In most cases, you wan't want or need to resort to manually pawing through log files. In most cases, you can use Ziggy for troubleshooting and to respond to problems.

[[Previous]](properties.md)
[[Up]](user-manual.md)
[[Next]](log-files.md)

