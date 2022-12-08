<!-- -*-visual-line-*- -->

<a href="doc/user-manual/user-manual.md">[Previous]</a> <a href="doc/user-manual/user-manual.md">[Up]</a> <a href="doc/user-manual/system-requirements.md">[Next]</a>

# Super-Basic Introduction

## What is Ziggy?

Ziggy is "A Pipeline management system for Data Analysis Pipelines."

What does that mean?

A data analysis pipeline is any data analysis software that proceeds in a step-by-step fashion, in which the  inputs to the later steps include (but are not limited to) the outputs from the earlier steps. The vast majority of science data analysis falls into this category.

A pipeline management system is all of the functionality that makes such a pipeline work. It's everything other than the actual software packages that do the processing. That includes execution of the algorithms on the data, but also such activities as: routing logging messages to the correct destinations; automatically executing the next step when the current step has completed; ensuring that the pipeline does the right thing when an exception occurs (either in Ziggy or in one of the algorithm packages); providing a user interface so that operators can control and monitor activities; managing a datastore of inputs and results; providing persistence for all of the records that need to be preserved across time; and much more.

## Why does Ziggy exist?

So -- why should anyone use Ziggy, or for that matter any other "pipeline management system" for their data analysis needs? Here's why:

Any data analysis activity that handles more than a trivial amount of data will require some sort of pipeline management system. At a minimum, it's going to be essential to ensure that all the data gets processed and that the processing is uniform, because otherwise any results from the processing become suspect: the user has to wonder, "If I missed processing some subset of the data, would that affect my results?" and, "If I didn't process all of the data the same way -- if I changed how I did the processing midway through my dataset -- will that affect my results?" Because of these issues, data analysis inevitably winds up applying some degree of automation to the process, even if it's just a handful of shell scripts that the user runs manually.

As data volumes get larger, the issue of managing the pipeline becomes more and more onerous, and more and more crucial. At the same time, development and maintenance of the pipeline manager becomes more and more of a distraction to the subject matter experts who just want to perform their data analysis, get their results, publish their papers, etc. At some point, rather than taking on the job of writing all this software that's not in their area of interest, the subject matter experts should look around for some existing software that will do all this management for them -- something that allows them to plug their processing application software into the management system and, presto! Complete system.

Ziggy is that "something."

## Where can I run Ziggy?

Ziggy is actually pretty lightweight. During development, we run Ziggy on some fairly standard laptops without any problems, so you shouldn't have any trouble downloading, building, and trying out Ziggy.

In terms of where you run Ziggy, what's more important than Ziggy itself is the data volume you need to process and the requirements you place on things like keeping the data in a location where mulitple users have access to it. Depending on the answers to those questions, you might be able to run your analysis on a laptop; a workstation; a server; or a cloud or high-performance computing (HPC) environment. Ziggy has been used in all of these different locations, based on the task at hand.

## When did Ziggy get its start?

Ziggy was originally written as the pipeline infrastructure (PI) component for the pipeline that processed data from NASA's Kepler mission, which used transit photometry to look for signals of planets circling distant stars. It was run on server clusters and the NASA Advanced Supercomputer (NAS) at NASA's Ames Research Center, and the original pipeline team was housed at Ames as well.

A few years later, Ames provided the data analysis pipeline for another transit photometry mission, the Transiting Exoplanet Survey Satellite (TESS). A more advanced version of the Kepler PI component was developed for TESS, and was named Spiffy ("Science pipeline infrastructure for you").

In the fullness of time, members of the Kepler and TESS team realized that there was an opportunity to take Spiffy and make it into a software package that was suitable for truly huge data volumes (terabytes per day, as compared to the terabytes per month rate of TESS) and easier to use than Spiffy or PI had been. This resulted in the development process that culminated in Ziggy.

## Why Ziggy?

The Ziggy authors are big fans of 80's rock bands and the explosions of styles and genres that occurred at that time. The album "The Rise and Fall of Ziggy Stardust and the Spiders from Mars" technically came out in 1972, but we would assert that this album was actually an example of 80's alternative rock, just one that happened to arrive almost a decade before the 1980's did. David Bowie was always ahead of his time that way.

## How do I put Ziggy to use?

Glad you asked that! The intent of the [user manual](doc/user-manual/user-manual.md) is to take you through the process, step by step. Thus, we recommend starting at the first link and clicking your way down as you make progress.

Additionally, Ziggy ships with a sample pipeline. This pipeline uses an extremely simple set of algorithms to demonstrate as much of Ziggy's prowess and features as possible. As we go through the steps, we'll show highlights from the sample pipeline so that you have a sense of what it is you should see. Anyway, it's always easier to explain something by example than to explain in a totally abstract way...

## Who maintains Ziggy?

Ziggy is a collaboration between the Science Processing Group in NASA’s Advanced Supercomputing Division and NASA’s Earth Exchange (NEX) in the Earth Science Division at NASA Ames Research Center. You can [Contact Us](doc/user-manual/contact-us.md).

## License

Copyright © 2022 United States Government as represented by the Administrator of the National Aeronautics and Space Administration. All Rights Reserved.

NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A, 80NSSC18M0068 & 80NSSC21M0079.

This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have received a copy of this agreement with the Ziggy source code; see the file [LICENSE.pdf](LICENSE.pdf).

Disclaimers

No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."

Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM, RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE, UNILATERAL TERMINATION OF THIS AGREEMENT.

## Other licenses

Ziggy makes use of third-party software. Their licenses appear in the [licenses](licenses/licenses.md) directory.

## Contributing

If you are interested in contributing to Ziggy, please complete the NASA license agreement that applies to you:

* [doc/legal/NASA-Corporate-CLA.pdf](doc/legal/NASA-Corporate-CLA.pdf)
* [doc/legal/NASA-Individual-CLA.pdf](doc/legal/NASA-Individual-CLA.pdf)



<a href="doc/user-manual/user-manual.md">[Previous]</a> <a href="doc/user-manual/user-manual.md">[Up]</a> <a href="doc/user-manual/system-requirements.md">[Next]</a>
