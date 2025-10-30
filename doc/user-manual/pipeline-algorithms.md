<!-- -*-visual-line-*- -->

[[Previous]](configuring-pipeline.md)
[[Up]](configuring-pipeline.md)
[[Next]](python-pipeline-algorithms.md)

## Pipeline Algorithms

For you, the user, pipeline algorithms are the most important part of the pipeline, because pipeline algorithms are the part that you write. This is where you implement your algorithms for execution by Ziggy. This necessarily means that there are specific requirements for how you write the code, and where the code is located on your file system, that you will have to follow so that Ziggy knows where they are and what to do with them. 

No need to panic, though. The requirements are not onerous (at least we think they're not).

### Pipeline Definition Element

The algorithms are represented in the XML by `step` elements. Here's the relevant block from [sample-pipeline.xml](../../sample-pipeline/etc/ziggy.d/sample-pipeline.xml):

```xml
<step name="permuter" file="major_tom/major_tom.py" description="Color Permuter"/>
<step name="flip" file="major_tom/major_tom.py" description="Flip Up-Down and Left-Right"/>
<step name="averaging" file="major_tom/major_tom.py" description="Average Images Together"/>
```

#### Why "`step`" and not "`algorithm`"?

Glad you asked!

The `step` XML element represents a pipeline step. For most users, most of the time, every `step` represents an algorithm. However! This is not always the case. There are some pipelines in use today that include steps that are not algorithms. 

You will probably never need to deal with steps that are not algorithms, so for you this is just what a wise man once called "some useless information, designed to drive my imagination."

#### Attributes of the `step` element

As you can see, there are three attributes to a `step` element. 

##### `name` Attribute

This is pretty much what it sounds like. This is the unique name that you will use in your pipelines to define the order in which pipeline steps are executed. 

###### Can I Include Whitespace in the Step Name?

Short answer: you can, but you probably shouldn't. 

Longer answer:

At runtime, the step name is used to create names for log files and for the task directory. Neither of these use-cases can tolerate whitespace because they interact with file systems that can take a dim view of whitespace in a file name. In order to match the round peg of a step name with whitespace into the square hole of the file system's expectations, all whitespace in the step name is replaced with underscore characters when creating the log files and task directories. Hence, your step name of "my step" will become "my_step" in the context of log files and task directories. 

This is already annoying enough, but it gets worse if you want to write some sort of script that loops over log files or over task directories to do post-processing on your pipeline results. At that point you'd need to remember to use the name with underscores rather than the original name of the pipeline step.

Anyway, don't use whitespace in a step name if you have any way to avoid it. 

##### `file` Attribute

This is the file that Ziggy will use to execute the pipeline step. This is actually an optional attribute. If you don't specify a `file`, Ziggy will look for an executable file that has the step's `name` attribute as the file name. 

In the `step` definitions above, if the file attributes were removed, Ziggy would look for a file named `permuter` that was a shell script or a binary or some such thing that Ziggy could run, and would run it. 

As you can see, the `file` attribute doesn't have to be unique. 

##### `description` Attribute

This is a text string that, well, describes what the `step` does. Ziggy doesn't use this information, it's just there to assist us poor humans to understand why we even have this `step` in the first place. 

### Where Do the Algorithm Executables Go?

In the last section we mentioned that Ziggy will "look for" an executable file. This begs the question: where does Ziggy look for this file? How does Ziggy know where to look? 

The short answer is: you tell it where to look. 

The longer answer is: if you look at the article on [the properties file](properties.md). you'll see a property named `ziggy.pipeline.binPath`. You set the value of this property to include all the places you want Ziggy to include when it looks for your executable. (Okay, the longer answer isn't that much longer than the short answer.)

The situation is slightly different when you're using Python algorithms that run in a Python environment. 

### Supported Algorithm Languages

In principle, Ziggy can support algorithms written in any language. Users have supplied algorithms in MATLAB, C++, Java, and Python; all have integrated into Ziggy, no fuss no muss. Furthermore, each step can be written in a different language without any problems; we've done a fair bit of that, as well.

For more complicated situations, like when an algorithm requires a bunch of setup and teardown, we've resorted to shell scripts that perform the setup, call the algorithm, and then perform the teardown.

Nonetheless, these days most algorithms we encounter are written in Python. Python requires a fair bit of custom hand-holding relative to other languages, in particular hand-holding related to virtual environments, but also more generally the entire package-module-function hierarchy. For this reason, we provide additional features to simplify life when writing Python code for execution in the pipeline.

For those of you who want to learn about Python modules, check out the next article in the manual. Folks who are more interested in any language other than Python can skip ahead to the article after that. 

[[Previous]](configuring-pipeline.md)
[[Up]](configuring-pipeline.md)
[[Next]](python-pipeline-algorithms.md)