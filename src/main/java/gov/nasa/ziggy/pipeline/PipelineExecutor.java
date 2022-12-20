package gov.nasa.ziggy.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.report.Memdrone;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceAggregateState;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ProcessingSummaryOperations;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventLabels;
import gov.nasa.ziggy.services.messages.RunningPipelinesCheckRequest;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.worker.WorkerPipelineProcess;

/***
 * Encapsulates the launch and transition logic for pipelines**
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineExecutor {
    private static final Logger log = LoggerFactory.getLogger(PipelineExecutor.class);

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;
    private PipelineInstanceNodeCrud pipelineInstanceNodeCrud;
    private PipelineTaskCrud pipelineTaskCrud;

    public PipelineExecutor() {

        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
        pipelineInstanceCrud = new PipelineInstanceCrud();
        pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        pipelineTaskCrud = new PipelineTaskCrud();
    }

    /**
     * Launch a new {@link PipelineInstance} for this {@link PipelineDefinition} with optional
     * startNode and/or endNode.
     */
    public InstanceAndTasks launch(PipelineDefinition pipeline, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode) {
        return launch(pipeline, instanceName, startNode, endNode, null);
    }

    /**
     * Launch a new {@link PipelineInstance} for this {@link PipelineDefinition} with optional
     * startNode and/or endNode, plus an optional {@link String} for the name of a
     * {@link ParameterSet} created by a {@link ZiggyEventHandler}. This allows the event handler to
     * transfer information to the pipeline that it needs at runtime.
     */
    public InstanceAndTasks launch(PipelineDefinition pipeline, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode,
        String eventHandlerParamSetName) {
        pipeline.buildPaths();
        List<PipelineTask> tasks = new LinkedList<>();

        /*
         * Lock the current version of the model registry and associate it with this pipeline
         * instance.
         */
        ModelCrud modelCrud = new ModelCrud();
        ModelRegistry modelRegistry = modelCrud.lockCurrentRegistry();

        int priority = pipeline.getInstancePriority();

        if (priority < PipelineInstance.HIGHEST_PRIORITY) {
            priority = PipelineInstance.HIGHEST_PRIORITY;
        }

        if (priority > PipelineInstance.LOWEST_PRIORITY) {
            priority = PipelineInstance.LOWEST_PRIORITY;
        }

        PipelineInstance instance = new PipelineInstance();
        instance.setName(instanceName);
        instance.setPipelineDefinition(pipeline);
        instance.setState(PipelineInstance.State.PROCESSING);
        instance.startExecutionClock();
        instance.setPriority(priority);
        instance.setModelRegistry(modelRegistry);

        /*
         * Set the pipeline instance params to the latest version of the name specified in the
         * trigger and lock the param set. Also, if the pipeline instance needs a parameter set from
         * a ZiggyEventHandler, add that to the parameters.
         */
        Map<ClassWrapper<Parameters>, ParameterSetName> triggerParamNames = pipeline
            .getPipelineParameterSetNames();
        if (eventHandlerParamSetName != null) {
            triggerParamNames.put(new ClassWrapper<Parameters>(ZiggyEventLabels.class),
                new ParameterSetName(eventHandlerParamSetName));
        }
        Map<ClassWrapper<Parameters>, ParameterSet> instanceParams = instance
            .getPipelineParameterSets();

        bindParameters(triggerParamNames, instanceParams);

        instance.setPipelineParameterSets(instanceParams);

        pipelineInstanceCrud.create(instance);

        if (startNode == null) {
            // start at the root
            log.info("Creating instance nodes (starting at root because startNode not set)");

            List<PipelineInstanceNode> rootInstanceNodes = new LinkedList<>();

            for (PipelineDefinitionNode definitionRootNode : pipeline.getRootNodes()) {
                PipelineInstanceNode rootInstanceNode = createInstanceNodes(instance, pipeline,
                    definitionRootNode, endNode);
                rootInstanceNodes.add(rootInstanceNode);
            }

            // make sure the new PipelineInstanceNodes are in the db for
            // launchNode, below
            DatabaseService.getInstance().flush();

            for (PipelineInstanceNode instanceNode : rootInstanceNodes) {
                tasks.addAll(launchNode(instanceNode));
            }
        } else {
            // start at the specified startNode
            log.info("Creating instance nodes (startNode set, so starting there instead of root)");

            PipelineInstanceNode startInstanceNode = createInstanceNodes(instance, pipeline,
                startNode, endNode);
            instance.setStartNode(startInstanceNode);

            PipelineDefinitionNode taskGeneratorNode = startInstanceNode.getPipelineDefinitionNode()
                .taskGeneratorNode();

            Map<ClassWrapper<Parameters>, ParameterSetName> uowModuleParamNames = taskGeneratorNode
                .getModuleParameterSetNames();
            Map<ClassWrapper<Parameters>, ParameterSet> uowModuleParams = new HashMap<>();
            bindParameters(uowModuleParamNames, uowModuleParams);

            // make sure the new PipelineInstanceNodes are in the db for
            // launchNode, below
            DatabaseService.getInstance().flush();

            tasks.addAll(launchNode(startInstanceNode, taskGeneratorNode, uowModuleParams));

        }

        return new InstanceAndTasks(instance, tasks);
    }

    public TaskCounts updateTaskCountsForCurrentNode(PipelineTask task,
        boolean currentTaskSuccessful) {
        log.info("currentTaskSuccessful: " + currentTaskSuccessful);

        long pipelineInstanceNodeId = task.getPipelineInstanceNode().getId();
        TaskCounts newTaskCounts;

        // This code obtains the lock on PI_PIPELINE_INST_NODE with 'select for
        // update'.
        // The lock is held until the next commit
        if (currentTaskSuccessful) {
            newTaskCounts = pipelineInstanceNodeCrud
                .incrementCompletedTaskCount(pipelineInstanceNodeId);
        } else {
            newTaskCounts = pipelineInstanceNodeCrud
                .incrementFailedTaskCount(pipelineInstanceNodeId);
        }

        return newTaskCounts;
    }

    /**
     * The transition logic generates the worker task request messages for the next module in this
     * pipeline.
     * <p>
     * This method should only be called if the current task completed successfully.
     *
     * @param instance
     * @param task
     * @throws PipelineException
     */
    public LinkedList<PipelineTask> doTransition(PipelineInstance instance, PipelineTask task,
        TaskCounts currentNodeTaskCounts) {

        LinkedList<PipelineTask> tasks = new LinkedList<>();

        log.debug("doTransition(WorkerTaskRequest, PipelineInstanceNode) - start");
        log.debug("doTransition: current task = " + task.getId());
        log.info("task.isRetried(): " + task.isRetry());

        Memdrone memdrone = new Memdrone(task.getModuleName(), task.getPipelineInstance().getId());
        try {
            if (Memdrone.memdroneEnabled()) {
                memdrone.createStatsCache();
                memdrone.createPidMapCache();
            }
        } catch (Exception e) {
            throw new PipelineException("Memdrone processing failed with exception", e);
        }

        PipelineInstanceNode instanceNode = pipelineInstanceNodeCrud.retrieve(instance,
            task.getPipelineDefinitionNode());

        log.info("doTransition: instanceNode " + currentNodeTaskCounts.log());

        // @formatter:off
        /**
         * (using javadoc-style comments to keep eclipse from munging
         * formatting)
         *
         * <pre>
         *
         * if there is another node in this pipeline,
         *   for each nextNode
         *     if nextNode.isStartNewUow() == true
         *       if all tasks for this node are complete
         *         use the task generator for nextNode to generate the next set of tasks
         *       else
         *         create a new task with the same uowTask as the last task
         * else
         *   pipeline complete for this UOW
         *
         * </pre>
         */
        //@ formatter:on

        // true if the user specified an optional end node (not null and equal
        // to the current node)
        PipelineInstanceNode endNode = instance.getEndNode();
        boolean isEndNode = endNode != null && instanceNode.getId() == endNode.getId();

        if (!isEndNode && task.getPipelineDefinitionNode().getNextNodes().size() > 0) {
            log.debug("more nodes remaining for this pipeline");
            for (PipelineDefinitionNode nextDefinitionNode : task.getPipelineDefinitionNode()
                .getNextNodes()) {
                PipelineInstanceNode nextInstanceNode = pipelineInstanceNodeCrud.retrieve(instance,
                    nextDefinitionNode);

                if (nextDefinitionNode.isStartNewUow()) { // synchronized
                    // transition
                    log.debug("isWaitForPreviousTasks == true, checking to see if all tasks for this node are complete");

                    if (currentNodeTaskCounts.isInstanceNodeComplete()) {
                        log.info("doTransition: all tasks for this node done");


                        log.info("doTransition: launching next node with a new UOW");
                        tasks.addAll(launchNode(nextInstanceNode));
                    } else {
                        log.info("doTransition: there are uncompleted tasks remaining for this node, doing nothing");
                    }
                } else {
                    /*
                     * Simple transition: just propagate the last task to the
                     * nextNode
                     */

                    log.info("doTransition: nextNode uses the same UOW, just creating a single task with the UOW from this task");

                    BeanWrapper<UnitOfWork> nextUowTask = null;

                    if (task.getUowTask() != null) {
                        nextUowTask = new BeanWrapper<>(task.getUowTask().getInstance());
                    }

                    tasks.add(launchTask(nextInstanceNode, instance, nextUowTask));
                    pipelineInstanceNodeCrud.incrementSubmittedTaskCount(nextInstanceNode.getId());
                }
            }
        } else {
            if (isEndNode) {
                log.info("doTransition: end of pipeline reached for this UOW (reached specified endNode)");
            }

            log.info("doTransition: end of pipeline reached for this UOW");
        }

        return tasks;
    }

    /**
     * Updates the PipelineInstance state based on the aggregate
     * PipelineInstanceNode task counts. Called after the transition logic runs.
     *
     * @param instance
     */
    public void updateInstanceState(PipelineInstance instance) {
        PipelineInstanceAggregateState state = pipelineInstanceCrud.instanceState(instance);

        if (state.getNumCompletedTasks().equals(state.getNumTasks())) {
            // completed successfully
            instance.setState(PipelineInstance.State.COMPLETED);
            instance.stopExecutionClock();
        } else if (state.getNumFailedTasks() > 0) {
            if (state.getNumFailedTasks() + state.getNumCompletedTasks() == state
                .getNumSubmittedTasks()) {
                instance.setState(PipelineInstance.State.ERRORS_STALLED);
                instance.stopExecutionClock();
            } else {
                instance.setState(PipelineInstance.State.ERRORS_RUNNING);
            }
        } else {
            // situation normal
            instance.setState(PipelineInstance.State.PROCESSING);
        }

        log.info("updateInstanceState: all nodes: numTasks/numSubmittedTasks/numCompletedTasks/numFailedTasks =  "
            + state.getNumTasks()
            + "/"
            + state.getNumSubmittedTasks()
            + "/"
            + state.getNumCompletedTasks() + "/" + state.getNumFailedTasks());

        log.info("updateInstanceState: updated PipelineInstance.state = " + instance.getState()
        + " for id: " + instance.getId());
    }

    /**
     * Restart a PipelineTask in the ERROR state. Usually called from the console.
     *
     * @param task
     * @param doTransitionOnly
     */
    public void restartFailedTask(PipelineTask task, boolean doTransitionOnly) {
        log.debug("runTask(long) - start");

        boolean okayToRestart = false;
        PipelineTask.State oldState = task.getState();
        if (oldState == PipelineTask.State.ERROR) {
            okayToRestart = true ;
        } else {
            ProcessingSummary processingState = new ProcessingSummaryOperations()
                .processingSummary(task.getId());
            if (processingState.getProcessingState() == ProcessingState.COMPLETE &&
                processingState.getFailedSubtaskCount() > 0) {
                okayToRestart = true ;
            }
        }

        log.info("Restarting failed task id=" + task.getId() + ", oldState : " + oldState);

        if (!okayToRestart) {
            throw new PipelineException("Can only restart ERROR tasks or COMPLETE tasks with failed sub-tasks!  state = " + oldState);
        }

        // Retrieve the task so that it can be modified in the database using the Hibernate infrastructure
        PipelineTask databaseTask = pipelineTaskCrud.retrieve(task.getId());
        databaseTask.setState(PipelineTask.State.SUBMITTED);
        databaseTask.startExecutionClock();
        databaseTask.setRetry(true);

        PipelineInstance instance = databaseTask.getPipelineInstance();
        PipelineInstanceNode instanceNode = pipelineInstanceNodeCrud.retrieve(instance,
            task.getPipelineDefinitionNode());

        log.info("restartFailedTask: currentNode: numTasks/numSubmittedTasks/numCompletedTasks/numFailedTasks =  "
            + instanceNode.getNumTasks()
            + "/"
            + instanceNode.getNumSubmittedTasks()
            + "/"
            + instanceNode.getNumCompletedTasks() + "/" + instanceNode.getNumFailedTasks());

        pipelineInstanceNodeCrud.decrementFailedTaskCount(instanceNode.getId());

        updateInstanceState(instance);
        pipelineInstanceCrud.update(instance);
        pipelineTaskCrud.update(databaseTask);

        restartTask(task, doTransitionOnly);
    }

    /**
     * Re-submit a task to the worker queue without making any changes to the {@link PipelineTask}
     * object in the database.
     *
     * @param task
     * @param doTransitionOnly
     */
    public void restartTask(PipelineTask task, boolean doTransitionOnly) {
        PipelineTask.State oldState = task.getState();

        log.info("Restarting task id=" + task.getId() + ", oldState : " + oldState);
    }

    /**
     * Re-submit a task to the worker queue without making any changes to the {@link PipelineTask}
     * object in the database.
     *
     * @param task
     */
    public void restartTask(PipelineTask task) {
        restartTask(task, false);
    }

    /**
     * Generate and send out the tasks for the specified node.
     *
     * @param instanceNode
     * @param queueName
     * @return
     */
    private LinkedList<PipelineTask> launchNode(PipelineInstanceNode instanceNode) {
        Map<ClassWrapper<Parameters>, ParameterSet> uowModuleParameterSets = instanceNode
            .getModuleParameterSets();
        PipelineDefinitionNode taskGeneratorNode = instanceNode
            .getPipelineDefinitionNode().taskGeneratorNode();

        return launchNode(instanceNode, taskGeneratorNode, uowModuleParameterSets);
    }

    /**
     * Generate and send out the tasks for the specified node.
     *
     * @param instanceNode
     * @param queueName
     * @param uowModuleParameterSets
     * @return
     */
    private LinkedList<PipelineTask> launchNode(PipelineInstanceNode instanceNode,
        PipelineDefinitionNode taskGeneratorNode,
        Map<ClassWrapper<Parameters>, ParameterSet> uowModuleParameterSets) {
        PipelineInstance instance = instanceNode.getPipelineInstance();
        PipelineDefinitionNode definitionNode = instanceNode.getPipelineDefinitionNode();
        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = unitOfWorkGenerator(taskGeneratorNode);


        if (!unitOfWorkGenerator.isInitialized()) {
            throw new PipelineException(
                "Configuration Error: Unable to launch node because no UnitOfWorkGenerator class is defined");
        }

        Map<ClassWrapper<Parameters>, ParameterSet> pipelineParameterSets = instance
            .getPipelineParameterSets();

        /*
         * Create a Map containing all of the entries from the pipeline
         * parameters plus the module parameters for this node for use by the
         * UnitOfWorkTaskGenerator. This allows the UOW parameters to be
         * specified at either the pipeline or module level.
         */
        Map<ClassWrapper<Parameters>, ParameterSet> compositeParameterSets = new HashMap<>(
            pipelineParameterSets);

        for (ClassWrapper<Parameters> moduleParameterClass : uowModuleParameterSets.keySet()) {
            if (compositeParameterSets.containsKey(moduleParameterClass)) {
                throw new PipelineException(
                    "Configuration Error: Module parameter and pipeline parameter Maps both contain a value for parameter class: "
                        + moduleParameterClass);
            }
            compositeParameterSets.put(moduleParameterClass,
                uowModuleParameterSets.get(moduleParameterClass));
        }

        Map<Class<? extends Parameters>, Parameters> uowParams = new HashMap<>();

        for (ClassWrapper<Parameters> parametersClass : compositeParameterSets.keySet()) {
            ParameterSet parameterSet = compositeParameterSets.get(parametersClass);
            Class<? extends Parameters> clazz = parametersClass.getClazz();
            uowParams.put(clazz, parameterSet.parametersInstance());
        }

        List<UnitOfWork> tasks = new ArrayList<>();
        tasks = unitOfWorkGenerator.newInstance().generateUnitsOfWork(uowParams);

        if (tasks.isEmpty()) {
            throw new PipelineException(
                "Task generation did not generate any tasks!  UOW class: "
                    + unitOfWorkGenerator.getClassName());
        }

        LinkedList<PipelineTask> pipelineTasks = new LinkedList<>();

        for (UnitOfWork task : tasks) {
            pipelineTasks.add(launchTask(instanceNode, instance, new BeanWrapper<>(
                task)));
        }

        instanceNode.setNumTasks(tasks.size());
        instanceNode.setNumSubmittedTasks(tasks.size());

        PipelineInstanceNode endNode = instance.getEndNode();
        if (endNode == null || instanceNode.getId() != endNode.getId()) {
            propagateTaskCount(instance, definitionNode.getNextNodes(), tasks.size());
        }

        return pipelineTasks;
    }

    public ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineDefinitionNode node) {
        return UnitOfWorkGenerator.unitOfWorkGenerator(node);
    }

    /**
     * Recursive method to create a {@link PipelineInstanceNode} for all
     * subsequent {@link PipelineDefinitionNode}s
     *
     * This is later used by PipelineInstanceCrud.isNodeComplete() to determine
     * if all tasks are complete for a given node.
     *
     * @param instance
     * @param pipeline
     * @param definitionNodes
     * @param taskCount
     * @throws PipelineException
     */
    private PipelineInstanceNode createInstanceNodes(PipelineInstance instance,
        PipelineDefinition pipeline, PipelineDefinitionNode node, PipelineDefinitionNode endNode) {
        PipelineModuleDefinition moduleDefinition = pipelineModuleDefinitionCrud
            .retrieveLatestVersionForName(node.getModuleName());
        moduleDefinition.lock();

        PipelineInstanceNode instanceNode = new PipelineInstanceNode(instance, node,
            moduleDefinition);
        pipelineInstanceNodeCrud.create(instanceNode);

        Map<ClassWrapper<Parameters>, ParameterSetName> pipelineNodeParameters =
            node.getModuleParameterSetNames();
        Map<ClassWrapper<Parameters>, ParameterSet> instanceNodeParams = instanceNode
            .getModuleParameterSets();

        bindParameters(pipelineNodeParameters, instanceNodeParams);

        if (endNode != null && endNode.getId() == node.getId()) {
            log.info("Reached optional endNode, not creating any more PipelineInstanceNodes");
            instance.setEndNode(instanceNode);
        } else {
            for (PipelineDefinitionNode nextNode : node.getNextNodes()) {
                createInstanceNodes(instance, pipeline, nextNode, endNode);
            }
        }

        return instanceNode;
    }

    /**
     * For each {@link ParamSetName}, retrieve the latest version of the
     * {@link ParamSet}, lock it, and put it into the params map. Used for both
     * pipeline params and module params.
     *
     * @param paramNames
     * @param params
     */
    private void bindParameters(Map<ClassWrapper<Parameters>, ParameterSetName> paramNames,
        Map<ClassWrapper<Parameters>, ParameterSet> params) {
        for (ClassWrapper<Parameters> paramClass : paramNames.keySet()) {
            ParameterSetName pipelineParamName = paramNames.get(paramClass);
            ParameterSet paramSet = parameterSetCrud
                .retrieveLatestVersionForName(pipelineParamName);
            params.put(paramClass, paramSet);
            paramSet.lock();
        }
    }

    /**
     * Create a new {@link PipelineTask} and the corresponding
     * {@link WorkerTaskRequest} message.
     *
     * @param instanceNode
     * @param queueName
     * @param instance
     * @param definitionNode
     * @param task
     * @return
     */
    private PipelineTask launchTask(PipelineInstanceNode instanceNode, PipelineInstance instance,
        BeanWrapper<UnitOfWork> task) {
        PipelineTask pipelineTask = new PipelineTask(instance, instanceNode);
        pipelineTask.setState(PipelineTask.State.SUBMITTED);
        pipelineTask.setUowTask(task);

        pipelineTaskCrud.create(pipelineTask);

        return pipelineTask;
    }

    /**
     * Propagate numTasks to later instance nodes that share the same UOW
     *
     * @param nextDefinitionNodes
     * @param numTasks
     */
    private void propagateTaskCount(PipelineInstance instance,
        List<PipelineDefinitionNode> nextDefinitionNodes, int numTasks) {
        for (PipelineDefinitionNode nextDefinitionNode : nextDefinitionNodes) {
            if (!nextDefinitionNode.isStartNewUow()) {
                PipelineInstanceNode instanceNode = pipelineInstanceNodeCrud.retrieve(instance,
                    nextDefinitionNode);
                instanceNode.setNumTasks(numTasks);

                PipelineInstanceNode endNode = instance.getEndNode();
                if (endNode == null || instanceNode.getId() != endNode.getId()) {
                    propagateTaskCount(instance, nextDefinitionNode.getNextNodes(), numTasks);
                }
            }
        }
    }

    public void sendWorkerMessageForTask(PipelineTask task) {
        sendWorkerMessageForTask(task, task.getPipelineInstance().getPriority(), false);
    }

    public void sendWorkerMessageForTask(PipelineTask task, int priority) {
        sendWorkerMessageForTask(task, priority, false);
    }

    /**
     * @param task
     * @throws PipelineException
     *
     * Note: the doTransitionOnly arg is heritage from the Kepler and TESS source (PI and Spiffy,
     * respectively). AFAICT, there is no use-case anywhere in our code for setting doTransitionOnly to
     * true. I'm keeping the option here just in case.
     */
    public void sendWorkerMessageForTask(PipelineTask task, int priority, boolean doTransitionOnly) {
    	sendWorkerMessageForTask(task, priority, doTransitionOnly, RunMode.STANDARD);
    }

    public void sendWorkerMessageForTask(PipelineTask task, int priority, boolean doTransitionOnly,
    		RunMode runMode) {

        log.debug("Generating worker task message for task=" + task.getId() + ", module="
                + task.getModuleName());

            WorkerTaskRequest workerTaskRequest = new WorkerTaskRequest(task.pipelineInstanceId(),
                task.pipelineInstanceNodeId(), task.getId(), priority, doTransitionOnly,
                runMode);

            WorkerPipelineProcess.workerTaskRequestQueue.put(workerTaskRequest);

    }

    /**
     * Sends a fire-trigger request message to the worker.
     */
    public void sendTriggerMessage(WorkerFireTriggerRequest triggerRequest) {
        UiCommunicator.send(triggerRequest);
    }

    /**
     * Sends a request for information on whether any pipelines are running or queued.
     */
    public void sendRunningPipelinesCheckRequestMessage() {
        log.info("Sending message to request status of running instances");
        UiCommunicator.send(new RunningPipelinesCheckRequest());
    }

    /**
     * For mocking purposes only
     *
     * @param pipelineInstanceCrud the pipelineInstanceCrud to set
     */
    public void setPipelineInstanceCrud(PipelineInstanceCrud pipelineInstanceCrud) {
        this.pipelineInstanceCrud = pipelineInstanceCrud;
    }

    /**
     * For mocking purposes only
     *
     * @param pipelineTaskCrud the pipelineTaskCrud to set
     */
    public void setPipelineTaskCrud(PipelineTaskCrud pipelineTaskCrud) {
        this.pipelineTaskCrud = pipelineTaskCrud;
    }

    public void setPipelineInstanceNodeCrud(PipelineInstanceNodeCrud pipelineInstanceNodeCrud) {
        this.pipelineInstanceNodeCrud = pipelineInstanceNodeCrud;
    }

    void setPipelineModuleDefinitionCrud(PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud) {
        this.pipelineModuleDefinitionCrud = pipelineModuleDefinitionCrud;
    }

    void setPipelineModuleParameterSetCrud(ParameterSetCrud parameterSetCrud) {
        this.parameterSetCrud = parameterSetCrud;
    }

}
