<!-- -*-visual-line-*- -->

[[Previous]](data-receipt-display.md)
[[Up]](user-manual.md)
[[Next]](event-handler-intro.md)

## Event Handler

Up to now, Ziggy has relied entirely on "operator in the loop" execution logic. In all the examples, you, the user / operator, has to do -- something -- to get Ziggy to take an action.

That's fine when dealing with a pipeline as simple-minded as the sample pipeline, but what happens when you've got a complex pipeline and a fire hose of mission data coming at you like Batman running at the Joker? For that matter, even if your pipeline is as simple as the sample pipeline, it would be a hassle if you were receiving data ten times a day and every time you had to manually import the data and start the pipeline.

For these reasons, Ziggy provides a capability that lets it operate without a user in the loop. In this case, Ziggy watches for an event that the user has defined, and when the event occurs Ziggy responds by starting a pipeline.

Here's how it works:

### [Introduction to Event Handlers](event-handler-intro.md)

Discussion of the concept and the design requirements that led us to the solution we implemented in Ziggy.

### [Defining Event Handlers](event-handler-definition.md)

How you go about defining your own event handler (spoiler alert: more XML).

### [Event Handler Examples](event-handler-examples.md)

Allows you to get some hands-on experience with the event handler.

### [Sending Event Information to Algorithms](event-handler-labels.md)

Got a use-case in which the processing algorithms need to know something about the event that set them running? We've got you covered.

[[Previous]](data-receipt-display.md)
[[Up]](user-manual.md)
[[Next]](event-handler-intro.md)
