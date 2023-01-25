<!-- -*-visual-line-*- -->

[[Previous]](data-receipt-display.md)
[[Up]](advanced-topics.md)
[[Next]](event-handler-definition.md)

## Event Handler: Introduction

Ziggy's predecessors in the pipeline infrastructure field (the Kepler SOC's PI module and TESS SPOC's Spiffy)  were designed to require a lot of in-person handling by pipeline users. In particular, they had no capacity to take actions without somebody, somewhere, issuing a command or pressing a button on the console. 

This design choice was acceptable for the aforementioned Kepler and TESS missions, which had data deliveries approximately once a month and always followed a constant sequence of processing activities. For future missions with much larger data rates, data deliveries are likely to be much more frequent, potentially every day or even more often. Future missions are also likely to have non-standard, high urgency processing requirements arise at unpredictable intervals (something along the lines of, "We need to process this chunk of data through this special-purpose pipeline, and we need to get it turned around in 12 hours"). Both of these expectations led to the decision that Ziggy required some means of taking autonomous action. In short, it needs an event handler: a system that allows the user to define some form of event that Ziggy should watch for, and an action that it should take when the event happens. 

### The Action

The action Ziggy takes in response to an event is pretty straightforward: it starts a pipeline. That's about all Ziggy is able to do at this time, so that's what it does. The user can define any specialized pipelines that are needed to respond to particular kinds of events.

### The Event

What are the necessary characteristics of an event? Put another way, what are the requirements we have that define an event?

From the outset, we knew that data receipt, or a pipeline that starts with data receipt and runs through all the various processing steps, was the event handler's "killer app." An event handler that can start the pipeline automatically when new data comes in relieves human beings of that burden, and if data comes in every day, or at unpredictable intervals, managing data delivery is a major burden indeed. We also knew that we didn't want the system design to tie the event handler so closely to data receipt that it couldn't be used for anything else. Thus we allowed data receipt to inform our design, while staying on the lookout for design choices that coupled the two systems too tightly together. 

With all that introductory material out of the way, we turn to the question: what are the requirements for the "event" part of the event handler?

- The event has to be something that unambiguously declares, "This event is ready to be handled!" In particular, this means that the event can't be a regular file that has content of some type (i.e., it can't be something like the manifest files used by data receipt). The problem with those files is that the file can appear on the system where Ziggy runs, but not be completely delivered (i.e., the file is there but the source is still copying the contents of the file across to the Ziggy system). In other words, the event must be atomic.
- The event needs to be similarly unambiguous about which pipeline Ziggy needs to start when the event happens. This is important because Ziggy might have a number of different kinds of events it wants to handle, and it needs to be able to clearly tell them apart.
- In the specific case of data receipt, the data source might deliver files to the main data receipt directory, or it might deliver subdirectories of files (see [the article on data receipt](data-receipt.md) for more information). Whatever system we devise must support either option.
- It may be the case that multiple data sources deliver separate data streams to Ziggy simultaneously. Ziggy needs to be able to manage such a situation. In particular, in the case of data delivery, Ziggy needs to be able to identify which subdirectories in the data receipt directory are from each data stream: each stream needs its own pipeline instance to perform the processing, and the different data streams cannot be expected to complete their deliveries simultaneously (i.e., Ziggy needs to know that one stream is done, and start processing that data, while also waiting for one or more other streams to complete). 
- The overall system needs to give the user some ability to monitor what's going on, whether progress is being made, etc. In the case of data receipt, this means that the user wants to know ahead of time how many directories to expect, and wants to know at runtime which ones are done and which are in progress. 

Based on the requirements above, we designed a solution as follows:

- Events are defined by zero-length files. In the case of data receipt, the data delivery source can push files to the system that hosts Ziggy, and once that file transfer is complete the source can create the necessary zero-length file. 
- Each event handler has a distinct directory that it watches for these zero-length files (henceforth we'll call these "ready files", because they indicate to Ziggy a degree of readiness). For data receipt, we traditionally use the data receipt directory (though we could have defined a totally separate directory for the ready files). For additional event handlers, the user would need to create other directories that can be watched for ready files, one directory per event handler. 
- An event can have more than one ready file, and Ziggy will only proceed when all the ready files for an event have appeared. Because a zero-length file has no content other than its name, the naming convention for ready files must encompass:
  - Some way of indicating which ready files go with a particular event.
  - The total number of ready files for a given event.
  - Additional information that is unique to each ready file: in the case of data receipt, since a ready file may be associated with a particular subdirectory in the data receipt directory, the name of the subdirectory needs to be in the name of the ready file. 

### The Ready File

Without further ado, here's the naming convention for the ready file:

```
<label>.READY.<name>.<count> 
```

Let's tackle these:

#### Name

This is the name of an event. All the ready files that go with a particular event have the same name. 

#### Count

The count, which is the part of the name that's furthest to the right, is the total number of ready files Ziggy should expect for this event. When the number of ready files matches this number, Ziggy will launch the appropriate pipeline.

#### READY

The presence of `READY` in all caps in the file name specifies that, well, this is a ready file. It provides an easy mechanism for a user to look at a directory and see which files are ready files, and also helps to prevent any confusion between ready files and other files that might have an otherwise-similar naming convention.

#### Label

The label is the part of the ready file that is unique within the set of ready files for an event. For the data receipt event handler, the label is the name of the data receipt subdirectory that goes with the ready file. For other types of events, the label can have some meaning related to the event.

Note that the label is optional. If, for example, a data source uses the main data receipt directory to deliver files, rather than a subdirectory, it will only have one ready file. In this case, there's no additional information needed by Ziggy to manage the event so the ready file can be `READY.<name>.1`. 

### Putting it All Together

Let's make this concrete with a data receipt example. Let's assume that there are two sources that are delivering data at the same time. One of the sources, by a convention agreed within the mission, always uses the event name `reeves-gabrels`; the other always uses the name `mick-ronson`. 

One particular day, `reeves-gabrels` sets up to deliver 5 subdirectories of data to the data receipt directory (specifically, `outside`, `earthling`, `hours`, `heathen`, `reality`), while `mick-ronson` sets up to deliver 3 subdirectories of data to the data receipt directory (specifically `world`, `hunky`, `stardust`). Let's assume that `reeves-gabrels` starts its delivery earlier than the `mick-ronson`. 

At some point, the data receipt directory will look like this:

```bash
outside
```

This is what we see when the first directory is in transit. Once it's done, reeves-gabrels places the first ready file, so the directory looks like:

```bash
outside
outside.READY.reeves-gabrels.5
```

What the user can tell from this is that:

- `reeves-gabrels` is going to deliver a total of 5 subdirectories. 
- The first of these, `outside`, is complete. 

As `reeves-gabrels` moves on to the next directory, we see this:

```bash
outside
outside.READY.reeves-gabrels.5
earthling
```

The second directory is in transit but not complete yet, because once it's complete there will be a ready file for that directory as well:

```bash
outside
outside.READY.reeves-gabrels.5
earthling
earthling.READY.reeves-gabrels.5
```

Time marches on and `reeves-gabrels` completes additional directories, meanwhile `mick-ronson` starts its delivery: 

```bash
outside
outside.READY.reeves-gabrels.5
earthling
earthling.READY.reeves-gabrels.5
hours
hours.READY.reeves-gabrels.5
heathen
heathen.READY.reeves-gabrels.5
reality
world
world.READY.mick-ronson.3
hunky
```

At about the same time that `mick-ronson` completes delivery of the `hunky` subdirectory, `reeves-gabrels` completes delivery of `reality`:

```bash
outside
outside.READY.reeves-gabrels.5
earthling
earthling.READY.reeves-gabrels.5
hours
hours.READY.reeves-gabrels.5
heathen
heathen.READY.reeves-gabrels.5
reality
reality.READY.reeves-gabrels.5
world
world.READY.mick-ronson.3
hunky
hunky.READY.mick-ronson.3
```

At this point, Ziggy knows that the `reeves-gabrels` event is complete and ready for processing, while the `mick-ronson` one is still working. It knows this because 5 out of 5 `reeves-gabrels` ready files are here, but only 2 out of 3 `mick-ronson` ones. Ziggy now kicks off the pipeline to process the `reeves-gabrels` deliveries, and the directory then looks like this:

```bash
outside
earthling
hours
heathen
reality
world
world.READY.mick-ronson.3
hunky
hunky.READY.mick-ronson.3
```

As soon as the pipeline starts, Ziggy deletes the ready files for reeves-gabrels. This prevents Ziggy from trying to respond to this event a second time. 

[[Previous]](data-receipt-display.md)
[[Up]](advanced-topics.md)
[[Next]](event-handler-definition.md)

