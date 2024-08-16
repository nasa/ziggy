/*
 * Copyright (C) 2022-2024 United States Government as represented by the Administrator of the
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

package gov.nasa.ziggy.pipeline;

import java.util.List;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

public class PipelineTaskDebugger {

    private static final Logger log = LoggerFactory.getLogger(PipelineTaskDebugger.class);

    private static final String TASK_DEBUGGER_PREFIX = "ptd.";
    private static final String PIPELINE_TASK_ID_PROP = TASK_DEBUGGER_PREFIX + "pipelineTaskId";
    private static final String DEBUG_ASYNC_ENABLED_PROP = TASK_DEBUGGER_PREFIX
        + "debugAsyncEnabled";
    private static final String DEBUG_FROM_START_ENABLED_PROP = TASK_DEBUGGER_PREFIX
        + "debugFromStartEnabled";
    private static final String DEBUG_LAUNCH_TRIGGER_ENABLED_PROP = TASK_DEBUGGER_PREFIX
        + "debugLaunchTriggerEnabled";
    private static final String TASK_DIR_PROP = TASK_DEBUGGER_PREFIX + "taskDir";
    private static final String PERSISTABLE_OUTPUT_CLASS_NAME_PROP = TASK_DEBUGGER_PREFIX
        + "persistableClassName";
    private static final String MODULE_NAME_PROP = TASK_DEBUGGER_PREFIX + "moduleName";
    private static final String TRIGGER_NAME_PROP = TASK_DEBUGGER_PREFIX + "triggerName";

    private int pipelineTaskId;
    private boolean debugAsyncEnabled;
    private boolean debugFromStartEnabled;
    private boolean debugLaunchTriggerEnabled;
    private String taskDir;
    private String persistableOutputClassName;
    private String moduleName;
    private String triggerName;

    /*
     * Either set properties, ptd.*, or provide explicit values as the defaults in the config calls
     * in this constructor.
     */
    public PipelineTaskDebugger(ImmutableConfiguration config) {
        // used by all
        pipelineTaskId = config.getInt(PIPELINE_TASK_ID_PROP, 35);
        debugAsyncEnabled = config.getBoolean(DEBUG_ASYNC_ENABLED_PROP, true);
        debugFromStartEnabled = config.getBoolean(DEBUG_FROM_START_ENABLED_PROP, false);
        debugLaunchTriggerEnabled = config.getBoolean(DEBUG_LAUNCH_TRIGGER_ENABLED_PROP, false);

        if (isDebugAsyncEnabled()) {
            persistableOutputClassName = config.getString(PERSISTABLE_OUTPUT_CLASS_NAME_PROP,
                "gov.nasa.kepler.pa.PaOutputs");
            taskDir = config.getString(TASK_DIR_PROP);
        } else if (isDebugLaunchTriggerEnabled()) {
            moduleName = config.getString(MODULE_NAME_PROP, "pa");
            triggerName = config.getString(TRIGGER_NAME_PROP, "PHOTOMETRY_LC");
        }
    }

    public static void main(String[] args) {
        PipelineTaskDebugger pipelineTaskDebugger = new PipelineTaskDebugger(
            ZiggyConfiguration.getInstance());
        startProcessingThread(pipelineTaskDebugger);
        log.info("Completed.");

        System.exit(0);
    }

    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    private static void startProcessingThread(final PipelineTaskDebugger pipelineTaskDebugger) {
        Thread thread = new Thread() {
            private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
            private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();

            @Override
            @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
            public void run() {
                try {
                    PipelineTask task = pipelineTaskOperations
                        .pipelineTask(pipelineTaskDebugger.getPipelineTaskId());

                    dumpConfig(pipelineTaskDebugger, task);
                    if (pipelineTaskDebugger.isDebugAsyncEnabled()) {
                        debugAsyncLocalFromProcessOutputs(task);
                    } else if (pipelineTaskDebugger.isDebugFromStartEnabled()) {
                        debugFromStartOfTask(task);
                    } else {
                        debugDoTransition(task);
                    }
                    if (pipelineTaskDebugger.isDebugLaunchTriggerEnabled()) {
                        debugLaunchTrigger();
                    }
                } catch (Throwable e) {
                    processException(e);
                }
            }

            private void debugFromStartOfTask(PipelineTask pipelineTask) throws Exception {
                PipelineModule pipelineModule = pipelineTaskOperations
                    .moduleImplementation(pipelineTask);
                pipelineModule.processTask();
            }

            private void debugAsyncLocalFromProcessOutputs(PipelineTask pipelineTask) {
                PipelineModule pipelineModule = pipelineTaskOperations
                    .moduleImplementation(pipelineTask);
                Object moduleName = pipelineModule.getModuleName();
                if (!(moduleName instanceof String)) {
                    throw new IllegalStateException(String.format(
                        "%s: invalid class, getModuleName method did not return String",
                        pipelineTaskOperations.pipelineModuleDefinition(pipelineTask)
                            .getPipelineModuleClass()
                            .getClassName()));
                }

                if (!(pipelineModule instanceof ExternalProcessPipelineModule)) {
                    log.error(
                        "Configured pipelineModule must implement ExternalProcessPipelineModule.");
                }
            }

            private void debugDoTransition(PipelineTask pipelineTask) {
                PipelineExecutor pipelineExecutor = new PipelineExecutor();
                pipelineExecutor.transitionToNextInstanceNode(
                    pipelineTaskOperations.pipelineInstanceNode(pipelineTask));
            }

            private void debugLaunchTrigger() {
                PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
                PipelineDefinition pipelineDefinition = pipelineDefinitionOperations
                    .pipelineDefinition(pipelineTaskDebugger.getTriggerName());

                PipelineDefinitionNode pipelineDefinitionNode = getPipelineDefinitionNode(
                    pipelineDefinitionOperations.rootNodes(pipelineDefinition));

                new PipelineExecutor().launch(pipelineDefinition, "instanceName",
                    pipelineDefinitionNode, pipelineDefinitionNode, null);
            }

            private PipelineDefinitionNode getPipelineDefinitionNode(
                List<PipelineDefinitionNode> nodes) {
                for (PipelineDefinitionNode node : nodes) {
                    if (node.getModuleName().equals(pipelineTaskDebugger.getModuleName())) {
                        return node;
                    }

                    PipelineDefinitionNode nodeFromRecursiveCall = getPipelineDefinitionNode(
                        pipelineDefinitionNodeOperations.nextNodes(node));
                    if (nodeFromRecursiveCall != null) {
                        return nodeFromRecursiveCall;
                    }
                }
                return null;
            }
        };
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            System.exit(2);
        }
    }

    private static void processException(Throwable e) {
        log.error("Caught exception:  ", e);
        e.printStackTrace();
        log.error("Terminated.");
        System.exit(1);
    }

    private static void dumpConfig(PipelineTaskDebugger taskDebugger, PipelineTask pipelineTask) {

        log.info(String.format("%s=%s\n", PIPELINE_TASK_ID_PROP, taskDebugger.getPipelineTaskId()));
        log.info(
            String.format("%s=%s\n", DEBUG_ASYNC_ENABLED_PROP, taskDebugger.isDebugAsyncEnabled()));
        log.info(String.format("%s=%s\n", DEBUG_FROM_START_ENABLED_PROP,
            taskDebugger.isDebugFromStartEnabled()));
        log.info(String.format("%s=%s\n", DEBUG_LAUNCH_TRIGGER_ENABLED_PROP,
            taskDebugger.isDebugLaunchTriggerEnabled()));
        log.info(String.format("%s=%s\n", TASK_DIR_PROP, taskDebugger.getTaskDir()));
        log.info(String.format("%s=%s\n", PERSISTABLE_OUTPUT_CLASS_NAME_PROP,
            taskDebugger.getPersistableOutputClassName()));
        log.info(String.format("%s=%s\n", MODULE_NAME_PROP, taskDebugger.getModuleName()));
        log.info(String.format("%s=%s\n", TRIGGER_NAME_PROP, taskDebugger.getTriggerName()));

        log.info(String.format("%s=%s\n", ZiggyConfiguration.PIPELINE_CONFIG_PATH_ENV,
            System.getenv(ZiggyConfiguration.PIPELINE_CONFIG_PATH_ENV)));

        PipelineModule pipelineModule = new PipelineTaskOperations()
            .moduleImplementation(pipelineTask);
        Object moduleName = pipelineModule.getModuleName();
        if (moduleName instanceof String) {
            log.info(String.format("MODULE_NAME=%s\n", (String) moduleName));
        }
    }

    public int getPipelineTaskId() {
        return pipelineTaskId;
    }

    public boolean isDebugAsyncEnabled() {
        return debugAsyncEnabled;
    }

    public boolean isDebugFromStartEnabled() {
        return debugFromStartEnabled;
    }

    public boolean isDebugLaunchTriggerEnabled() {
        return debugLaunchTriggerEnabled;
    }

    public String getTaskDir() {
        return taskDir;
    }

    public String getPersistableOutputClassName() {
        return persistableOutputClassName;
    }

    public String getPipelineProperties() {
        return ZiggyConfiguration.PIPELINE_CONFIG_PATH_ENV;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getTriggerName() {
        return triggerName;
    }
}
