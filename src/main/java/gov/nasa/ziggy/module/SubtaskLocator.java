/*
 * Copyright (C) 2022-2025 United States Government as represented by the Administrator of the
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

package gov.nasa.ziggy.module;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Locates the subtask or subtasks within a task that use a specified file as an input or an
 * instrument model.
 * <p>
 * Given a task directory and a filename, the {@link SubtaskLocator} identifies and prints to stdout
 * all the subtasks that use that filename as any kind of input. Note that the class only works on
 * tasks that use {@link DatastoreDirectoryPipelineInputs} to define the inputs.
 *
 * @author PT
 */
public class SubtaskLocator {

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void main(String[] args) {

        Option directoryOption = new Option("d", "directory", true, "Task directory");
        Option fileOption = new Option("f", "file", true, "Name of file for search");
        Options options = new Options();
        options.addOption(directoryOption);
        options.addOption(fileOption);

        CommandLine cmdLine;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            throw new PipelineException("Unable to parse options", e);
        }
        if (!cmdLine.hasOption(directoryOption.getOpt())
            || !cmdLine.hasOption(fileOption.getOpt())) {
            throw new PipelineException("SubtaskLocator requires both directory and file options");
        }

        String taskDir = cmdLine.getOptionValue(directoryOption.getOpt());
        String filename = cmdLine.getOptionValue(fileOption.getOpt());

        TaskConfiguration taskConfiguration = TaskConfiguration.deserialize(new File(taskDir));

        DatastoreDirectoryPipelineInputs inputs = new DatastoreDirectoryPipelineInputs();
        List<String> modelFilenames = inputs.getModelFilenames();

        Path taskDirPath = Paths.get(taskDir);
        for (int i = 0; i < taskConfiguration.getSubtaskCount(); i++) {

            Path subdirPath = SubtaskUtils.subtaskDirectory(taskDirPath, i);
            PipelineInputsOutputsUtils.readPipelineInputsFromDirectory(inputs,
                PipelineInputsOutputsUtils.moduleName(subdirPath.getParent()), subdirPath);
            List<String> filesForSubtask = inputs.getDataFilenames();
            if (filesForSubtask.contains(filename) || modelFilenames.contains(filename)) {
                System.out.println("Subtask " + i + " contains file " + filename);
            }
        }
    }
}
