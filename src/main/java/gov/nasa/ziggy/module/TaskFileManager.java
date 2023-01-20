/*
 * Copyright (C) 2022 United States Government as represented by the Administrator of the National
 * Aeronautics and Space Administration. All Rights Reserved.
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

/**
 * Manages the movement of data files between the task directory and the subtask directories.
 * <p>
 * At the start of subtask execution, an instance of a subclass of {@link PipelineInputs} is
 * instantiated, and its {@link PipelineInputs#populateSubTaskInputs()} method is called; this puts
 * the necessary data and metadata files for execution into the subtask working directory.
 * <p>
 * At the end of subtask execution, an instance of a subclass of {@link PipelineOutputs} is
 * instantiated, and its {@link PipelineOutputs#populateTaskResults()} and
 * {@link PipelineOutputs#setResultsState()} are called. The former method moves any results files
 * from the subtask directory to the task directory; the latter determines whether any results were
 * produced and sets an appropriate status.
 *
 * @author PT
 */
public final class TaskFileManager {

    public static void main(String[] args) {

        try {
            String fullyQualifiedClassName = args[0];

            Class<?> pipelineInputsOutputsClass = Class.forName(fullyQualifiedClassName);
            if (PipelineInputs.class.isAssignableFrom(pipelineInputsOutputsClass)) {
                PipelineInputs p = (PipelineInputs) pipelineInputsOutputsClass
                    .getDeclaredConstructor()
                    .newInstance();
                p.populateSubTaskInputs();
            } else if (PipelineOutputs.class.isAssignableFrom(pipelineInputsOutputsClass)) {
                PipelineOutputs pipelineOutputs = (PipelineOutputs) pipelineInputsOutputsClass
                    .getDeclaredConstructor()
                    .newInstance();
                pipelineOutputs.populateTaskResults();
                pipelineOutputs.setResultsState();
            } else {
                throw new ModuleFatalProcessingException("Class " + fullyQualifiedClassName
                    + " does not implement PipelineInputsOutputs");
            }
            System.exit(0);
        } catch (Throwable t) {
            System.out.println("ERROR: TaskFileManager execution failed, stack trace follows");
            t.printStackTrace();
            System.exit(1);
        }
    }

}
