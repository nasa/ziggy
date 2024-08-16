<!-- -*-visual-line-*- -->

# Ziggy Release Notes

These are the release notes for Ziggy. In the change log below, we'll refer to our internal Jira key for our benefit. If the item is associated with a resolved GitHub issue or pull request, we'll add a link to that. Changes that are incompatible with previous versions are marked below. While the major version is 0, we will be making breaking changes when bumping the minor number. However, once we hit version 1.0.0, incompatible changes will only be made when bumping the major number.

# v0.6.0: You never have to wonder what Ae 4 / 3 / 0 means again

We fixed a confusing aspect of the user interface and a ton of bugs while we continued to buy down decades of technical debt. You can now halt tasks or instances from the command-line interface (CLI). We improved pipeline definitions by making datastore definitions more flexible and providing for user-specified data receipt unit of work (UOW) generators.

## New Features

1. Combine task state and p-state on tasks display (ZIGGY-166)
1. Review Edit Trigger Validate button for deletion (ZIGGY-224)
1. Untangle table hierarchies in pipeline definitions (ZIGGY-268)
1. Put back the executableName attribute for pipeline modules (ZIGGY-281)
1. Remove DatabaseModelRegistry and ConsoleDatabaseModel (ZIGGY-311)
1. Merge CLI cancel and reset commands into a single halt command (ZIGGY-389)
1. Replace isEmpty() with isBlank() (ZIGGY-395)
1. Add regular expression support to data file type fileNameRegexp (ZIGGY-398)
1. Remove last circular dependency in the ui package (ZIGGY-407)
1. Support user-specified data receipt UOW generators (ZIGGY-408)
1. Clean up CRUD proxies in console (ZIGGY-410)
1. Move all database transactions to Operations classes (ZIGGY-419)
1. Clean up Parameters namespace (ZIGGY-421)

## Bug Fixes

1. Pipeline doesn't error out when no UOWs are generated (ZIGGY-304)
1. Remote ComputeNodeMaster instances exit prematurely (ZIGGY-353)
1. Fix deprecated use of DefaultExecutor (ZIGGY-399)
1. Can't rename a group (ZIGGY-402)
1. Alerts panel shows hostname, not localhost (ZIGGY-403)
1. A slow data receipt module freezes the console (ZIGGY-411)
1. RemoteJob infrastructure doesn't detect failed jobs are finished (ZIGGY-414)
1. Restarting local tasks with resubmit hangs (ZIGGY-415)
1. Rename creates a copy of the pipeline (ZIGGY-416)
1. Averaging module writes results in dataset/L3 (ZIGGY-417)
1. Copy command behaves inconsistently (ZIGGY-418)
1. Setting debug logging level on messaging package hoses RMI (ZIGGY-422)

# v0.5.0: A major overhaul of the datastore, some UI improvements, and documentation for the command-line interface

The title pretty much says it all. There was also a lot of internal refactoring to buy down more technical debt.

## New Features

1. Review javadoc warnings (ZIGGY-126)
1. Remove GUI code from ProcessHeartbeatManager (ZIGGY-261)
1. Move remote execution configuration from parameters to pipeline node definition (ZIGGY-280)
1. Remove timestamp from pipeline instance name when launched by event handler (ZIGGY-299)
1. Add architecture and queue information to remote execution dialog (ZIGGY-306)
1. Increase number of digits displayed by cost estimate (ZIGGY-352)
1. Functionality inherited from Spiffy (ZIGGY-364)
1. Create ZiggyCliTest and add documentation to user manual (ZIGGY-370)
1. Remove security packages (ZIGGY-371)
1. Eliminate default from unit of work machinery (ZIGGY-374)
1. Refactor Parameters a bit more (ZIGGY-378)
1. Redesign datastore and data file type APIs (ZIGGY-380)
1. Implement subqueries in ZiggyQuery (ZIGGY-381)
1. Simplify ZiggyConfiguration and reduce test fixtures (ZIGGY-382)

## Bug Fixes

1. Max worker count not set correctly (ZIGGY-302)
1. Timestamp fields not initialized (ZIGGY-375)
1. Reserved queue dialog issues (ZIGGY-377)
1. Exceptions running ziggy.pl with missing properties (ZIGGY-391)
1. Ziggy supervisor logging no longer goes to supervisor.log (ZIGGY-392)

# v0.4.1: Fixed halt task and restart task commands

As promised, the Halt tasks (formerly Kill tasks) and Restart tasks commands have been fixed, along with a handful other buglets.

## New Features

## Bug Fixes

1. Remove option of UOW reuse (Incompatible change, ZIGGY-278)
1. Replace == with equals for non-primitives (ZIGGY-287)
1. Can't kill (local) tasks (ZIGGY-290)
1. Can't restart tasks (ZIGGY-291)
1. Incorrect number of downstream tasks (ZIGGY-303)
1. Increase pause after starting database (ZIGGY-354)
1. Undesired reprocessing (ZIGGY-361)
1. Tests sometimes fail in Eclipse (ZIGGY-367)
1. Unable to kill or restart tasks for first pipeline instance (ZIGGY-368)
1. Close and Cancel buttons in wrong order on resources dialogs (ZIGGY-372)
1. No transition after error resume (ZIGGY-373)

# v0.4.0: Hibernate 6, reorganized properties, an improved UI, runjava renamed to ziggy

Last time we said that our next release will contain the result of replacing our TESS data analysis pipeline infrastructure with Ziggy. That work continues, so we'll try to get back into a regular cadence of releases to avoid astronomically large releases, like this one.

As promised, we reorganized our properties and eliminated the effects of 15 years of entropy. We also updated the version of Hibernate we use and updated the database schema.

The UI witnessed a major facelift, and `runjava` was renamed to `ziggy` and the `--help` option works consistently with ziggy and its commands. The sample pipeline now uses an internal HSQLDB database, so it's even easier to try.

The version generator was redone to avoid rebuilding the world every time. Third-party sources have been moved from `buildSrc` to `outside,` where they are still safe from `gradle clean`.

The `Kill tasks` and `Restart tasks` commands are broken and will be fixed shortly in 0.4.1.

## New Features

1. Switch to Hibernate 6 (Incompatible change, ZIGGY-5)
1. Add additional queue support (ZIGGY-92)
1. Review handling of checked exceptions (ZIGGY-149)
1. Clean up of StateFile name / path management (ZIGGY-152)
1. Respond to requested changes on Ziggy remote execution dialog (ZIGGY-167)
1. Reorganization of console tabs and content (ZIGGY-169)
1. Clean up properties (Incompatible change, ZIGGY-172)
1. Replace ZiggyVersionGenerator / ZiggyVersion classes (ZIGGY-181)
1. Implement parameter set groups (ZIGGY-221)
1. Revise pipeline priorities (ZIGGY-225)
1. Changes needed for TESS-Ziggy (ZIGGY-240)
1. Implement subworkers (ZIGGY-242)
1. Add per-module thread settings (ZIGGY-243)
1. Switch sample pipeline to HSQLDB (ZIGGY-250)
1. Second Generation Messaging System (ZIGGY-259)
1. Refactor OpsInstancesPanel (ZIGGY-263)
1. Remove BeanWrappers class (ZIGGY-301)

## Bug Fixes

1. runjava <nickname> --help should provide help for <nickname> (ZIGGY-155)
1. Incorrect version of wrapper jarfile (ZIGGY-196)
1. Configuration is mutable and use of system properties is misleading (ZIGGY-201)
1. cluster init --force can't delete write-protected files (ZIGGY-231)
1. Fix Remote Execution typical wall time label (ZIGGY-232)
1. Remote parameter set not updated by remote execution dialog (ZIGGY-233)
1. Ziggy FileUtil.cleanDirectoryTree() fails with symlinks (ZIGGY-236)
1. ZiggyTable wrapping text doesn't support text color (ZIGGY-246)
1. A little time in the great outdoors, er, outside (ZIGGY-247)
1. Improve runjava console user experience (ZIGGY-251)
1. Database classes have ambiguous versioning (ZIGGY-256)
1. Remove task counts from PipelineInstanceNode (ZIGGY-262)
1. Delay task request messages until tasks are committed to database (ZIGGY-269)
1. Unable to start pipeline from selected node (ZIGGY-270)
1. Allow ZiggyCppPojo to use correct compiler for a given source file (ZIGGY-279)
1. Editing a parameter clobbers other parameters in the set (ZIGGY-286)

# v0.3.1: Fixed CITATION.cff and some news

This release includes a fix to our CITATION.cff courtesy of @dieghernan.

We are now working towards replacing our [TESS](https://www.nasa.gov/tess-transiting-exoplanet-survey-satellite) data analysis pipeline infrastructure with Ziggy. Our next release will contain the result of that work. It's a large effort and we expect it to take at least two months if not longer.

We are still planning to reorganize our properties and eliminate the effects of 15 years of entropy. We'll also be updating the version of Hibernate we use and anticipate updating the database schema as a result. You have been warned! This will occur before the 1.0 release to maximize our chances of stability after that.

## New Features

## Bug Fixes

1. Correct conference section in CITATION.cff (ZIGGY-241, [pull \#1](https://github.com/nasa/ziggy/pull/1))

# v0.3.0: Java 17, Gradle 7, and a new event manager

This release includes an update to the Gradle build system and an upgrade of Java to Java 17. It introduces an event manager system so that Ziggy can respond automatically to external events. The user manual was expanded to cover this feature and a few others.

We are planning to reorganize our properties and eliminate the effects of 15 years of entropy. We'll also be updating the version of Hibernate we use and anticipate updating the database schema as a result. You have been warned! This will occur before the 1.0 release to maximize our chances of stability after that.

## New Features

1. Upgrade to Java 17 (Incompatible change, ZIGGY-22)
1. Add static analysis to build (ZIGGY-30)
1. Create prototype event manager system (ZIGGY-119)
1. Clean up unit test execution on Gradle (ZIGGY-180)
1. Create user manual, part II (ZIGGY-193)
1. Remove Sockets from Subtask Server and Client (ZIGGY-199)
1. Update copyright (ZIGGY-220)

## Bug Fixes

1. Fix build warnings in both src and buildSrc (ZIGGY-142)
1. Delete setPosixPermissionsRecursively with a single permission (ZIGGY-197)
1. Ziggy "keep-up" processing fails (ZIGGY-204)
1. Correct P1 bugs identified by SpotBugs (ZIGGY-207)
1. NAS does not support Java 17 (Incompatible change, ZIGGY-234)

# v0.2.2: More documentation goodness

This release adds Previous, Next, and Up buttons to the user manual to make it easier to read cover to cover. We also added a CITATION.cff file to make it easier for you to cite Ziggy in your own work. Finally, we changed some 3-byte quotes to ASCII as these quotes could not be compiled if LANG is C.

We have updated the Gradle build system and Java to Java 17. This change will appear in version 0.3.0.

## New Features

1. Add "Prev", "Next", and "Up" buttons to user manual articles (ZIGGY-160)
1. Update GitHub documentation (ZIGGY-200)

## Bug Fixes

1. Fix funny quotes in NASA notices (ZIGGY-188)

# v0.2.1: Add GitHub info to docs

Once we uploaded our first version to GitHub, we could fill in some documentation TODOs like how to download the code.

We are still in the process of updating the build system Gradle and Java to at least Java 11 and possibly Java 17. We'll take advantage of post-Java 8 versions at that time. This change will appear in version 0.3.0.

## New Features

1. Ziggy reuse (ZIGGY-134)
1. Update GitHub info in manual (ZIGGY-192)

## Bug Fixes

# v0.2.0: Initial release

This is the first Ziggy release to appear on GitHub. The major number of 0 indicates that we're still refactoring the Kepler and TESS codebases and reserve the right to make breaking changes from time to time as we make the pipeline more widely useful. However, the general pipeline infrastructure has been in production use since Kepler's launch in 2009.

This is the first Ziggy release.

We are in the process of updating the build system Gradle and Java to at least Java 11. It's possible that we will take advantage of post-Java 8 versions at that time. This change will appear in version 0.3.0.

# 0.1.0

This release was overtaken by events. This release came and went before we got authorization to push our code to GitHub.
