<!-- -*-visual-line-*- -->

[[Previous]](event-handler-examples.md)
[[Up]](advanced-topics.md)
[[Next]](event-handler-labels.md)

## Event Handler: Algorithms

The event handler, and all of the information associated with it, is mainly intended for use by Ziggy: Ziggy uses the information to select a pipeline to start, to figure out which data receipt directories it can import, etc. That said, it's possible that event information can be useful to the algorithms as well. In the case when an algorithm runs in a pipeline that got kicked off by an event, there can be some value in giving the algorithm information about the handler and the event. 

If that's the case for your application, we've got you covered. 

### Event Information in the Algorithm Inputs

Recall that every subtask has its own inputs file, in HDF5 format (see the article on [Configuring a Pipeline](configuring-pipeline.md) for more information). Thus each permuter subtask has its own `permuter-inputs-0.h5` file. 

When a task is created by a pipeline that started in response to an event, the `moduleParameters` field in that task's inputs has a subfield named `ZiggyEventLabels`. The `ZiggyEventLabels` parameter set has, in turn, the following fields:

- `eventHandlerName`: the name of the event handler that kicked off this processing activity (in the case of our examples, `data-receipt`).
- `eventName`: the value of the name portion of the ready files (in our examples, `test1` and `test2`).
- `eventLabels`: a list of the ready file labels for this event. Note that every subtask sees all the labels that were part of the given event. In our multi-directory import example, one of the labels (`sample-1`) went with some tasks, the other label (`sample-2`) went with some other tasks. Ziggy doesn't have any way to figure out this relationship, so it just gives every run of the algorithm all of the labels. 
