/*
 * Copyright (C) 2022-2026 United States Government as represented by the Administrator of the
 * National Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline
 * Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A,
 * 80NSSC18M0068 & 80NSSC21M0079.
 *
 * This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have
 * received a copy of this agreement with the Ziggy source code; see the file LICENSE.pdf.
 *
 * Disclaimers
 *
 * No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER
 * EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT
 * SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE
 * ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT
 * SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY
 * OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY
 * OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY
 * DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE
 * ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."
 *
 * Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES
 * GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S
 * USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES
 * ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM,
 * RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
 * STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE
 * EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE,
 * UNILATERAL TERMINATION OF THIS AGREEMENT.
 */

package gov.nasa.ziggy.pipeline.step.subtask;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.TimestampFile;
import gov.nasa.ziggy.pipeline.step.hdf5.Hdf5AlgorithmInterface;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.SystemProxy;
import gov.nasa.ziggy.util.io.Persistable;

/**
 * Container class for wall times (in milliseconds) of subtasks for a given task. These are
 * accumulated at the end of task execution and stored in the subdirectory for the given task in the
 * run directory as an HDF5 file.
 *
 * @author PT
 */
public class AlgorithmWallTimes implements Persistable {

    private static final Logger log = LoggerFactory.getLogger(AlgorithmWallTimes.class);

    public static final String FILE_NAME = "subtask-walltimes.h5";
    private static final long HEARTBEAT_LOGGING_INTERVAL_MILLIS = 60_000L; // One minute.
    private static final String TASK_ID_OPT = "task-id";
    private static final String CSV_HEADER = "task,subtask,walltime";
    private static final String CSV_FORMAT = "%s,st-%d,%d";

    private List<SubtaskWallTime> subtaskWallTimes = new ArrayList<>();

    public void addWallTime(SubtaskWallTime wallTime) {
        subtaskWallTimes.add(wallTime);
    }

    public List<SubtaskWallTime> subtaskWallTimes() {
        return subtaskWallTimes;
    }

    /**
     * Walks through the subtask directories and determines the wall time for each, then saves the
     * whole collection as a {@link AlgorithmWallTimes} file.
     */
    public static void generateSubtaskWallTimesFile(PipelineTask pipelineTask) {

        Path taskDir = DirectoryProperties.taskDataDir().resolve(pipelineTask.taskBaseName());
        Path wallTimesFile = DirectoryProperties.runDir()
            .resolve(pipelineTask.taskBaseName())
            .resolve(FILE_NAME);
        AlgorithmWallTimes subtaskWallTimes = new AlgorithmWallTimes();

        List<Path> subtaskDirectories = SubtaskUtils.subtaskDirectories(taskDir);
        log.info("Obtaining wall times for {} subtasks of task {}...", subtaskDirectories.size(),
            taskDir.getFileName().toString());
        long lastHeartbeatLogTime = 0L;

        // If the process here takes a long time, issue a log message at regular intervals
        // so that the user knows that it's still running and hasn't bombed.
        for (Path subtaskDirectory : subtaskDirectories) {
            String subtaskDirectoryName = subtaskDirectory.getFileName().toString();
            if (SystemProxy.currentTimeMillis() > lastHeartbeatLogTime
                + HEARTBEAT_LOGGING_INTERVAL_MILLIS) {
                log.info("Processing subtask {}", subtaskDirectoryName);
                lastHeartbeatLogTime = SystemProxy.currentTimeMillis();
            }
            if (!TimestampFile.elapsedTimeValid(subtaskDirectory.toFile(),
                TimestampFile.Event.SUBTASK_START, TimestampFile.Event.SUBTASK_FINISH)) {
                log.warn("Subtask {} does not have valid start and finish files, skipping",
                    subtaskDirectoryName);
                continue;
            }
            subtaskWallTimes.addWallTime(new SubtaskWallTime(subtaskDirectory,
                TimestampFile.elapsedTimeMillis(subtaskDirectory.toFile(),
                    TimestampFile.Event.SUBTASK_START, TimestampFile.Event.SUBTASK_FINISH)));
        }
        log.info("Obtaining wall times for {} subtasks of task {}...done",
            subtaskDirectories.size(), taskDir.getFileName().toString());
        log.info("Writing wall times to file {} in directory {}...",
            wallTimesFile.getFileName().toString(), wallTimesFile.getParent().toString());
        try {
            Files.createDirectories(wallTimesFile.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        new Hdf5AlgorithmInterface().writeFile(wallTimesFile.toFile(), subtaskWallTimes, false);
        log.info("Writing wall times to file {} in directory {}...done",
            wallTimesFile.getFileName().toString(), wallTimesFile.getParent().toString());
    }

    /**
     * Reads the contents of the SubtaskWallTimes file for a given {@link PipelineTask} into an
     * instance of {@link AlgorithmWallTimes}.
     *
     * @return {@link AlgorithmWallTimes} instance if the wall times file is present, null
     * otherwise.
     */
    public static AlgorithmWallTimes readSubtaskWallTimesFile(PipelineTask pipelineTask) {
        Path wallTimesFile = DirectoryProperties.runDir()
            .resolve(pipelineTask.taskBaseName())
            .resolve(FILE_NAME);
        if (!Files.exists(wallTimesFile)) {
            return null;
        }
        AlgorithmWallTimes subtaskWallTimes = new AlgorithmWallTimes();
        log.info("Reading subtask wall times file for task {}...", pipelineTask.taskBaseName());
        new Hdf5AlgorithmInterface().readFile(wallTimesFile.toFile(), subtaskWallTimes, false);
        log.info("Reading subtask wall times file for task {}...done", pipelineTask.taskBaseName());
        return subtaskWallTimes;
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(TASK_ID_OPT, true, "Task ID of the wall times to be displayed");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Illegal argument: " + e.getMessage());
            usageAndExit(options);
        }
        if (!options.hasOption(TASK_ID_OPT)) {
            System.err.println("Task ID required");
            usageAndExit(options);
        }

        long taskId = Long.parseLong(cmdLine.getOptionValue(TASK_ID_OPT));
        PipelineTask pipelineTask = new PipelineTaskOperations().pipelineTask(taskId);
        if (pipelineTask == null) {
            throw new PipelineException("Task " + taskId + " does not exist");
        }

        AlgorithmWallTimes algorithmWallTimes = AlgorithmWallTimes
            .readSubtaskWallTimesFile(pipelineTask);
        if (algorithmWallTimes == null) {
            System.err.println("Task " + taskId + " has no wall times file");
            System.exit(-1);
        }
        System.out.println(CSV_HEADER);
        List<SubtaskWallTime> subtaskWallTimes = algorithmWallTimes.subtaskWallTimes();
        for (SubtaskWallTime subtaskWallTime : subtaskWallTimes) {
            System.out.println(String.format(CSV_FORMAT, pipelineTask.taskBaseName(),
                subtaskWallTime.getSubtaskIndex(), subtaskWallTime.getWallTimeMillis()));
        }
    }

    private static void usageAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("algorithm-wall-times", options);
        System.exit(-1);
    }

    /**
     * Container for a single subtask index and corresponding wall time. the
     * {@link #compareTo(SubtaskWallTime)} method will sort the instances of SubtaskWallTime into
     * descending order of wall times. This is based on the observation that the worst-performing
     * subtasks are much more interesting than the best.
     */
    public static class SubtaskWallTime implements Persistable, Comparable<SubtaskWallTime> {

        private final int subtaskIndex;
        private final long wallTimeMillis;

        public SubtaskWallTime() {
            subtaskIndex = -1;
            wallTimeMillis = -1;
        }

        public SubtaskWallTime(Path subtaskDir, long wallTimeMillis) {
            this.wallTimeMillis = wallTimeMillis;
            subtaskIndex = SubtaskUtils.subtaskIndex(subtaskDir);
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public long getWallTimeMillis() {
            return wallTimeMillis;
        }

        @Override
        public int compareTo(SubtaskWallTime o) {
            return (int) (o.getWallTimeMillis() - wallTimeMillis);
        }
    }
}
