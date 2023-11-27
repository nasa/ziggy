package gov.nasa.ziggy.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
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
import gov.nasa.ziggy.services.database.DatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.events.ZiggyEventLabels;
import gov.nasa.ziggy.services.messages.RemoveTaskFromKilledTasksMessage;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/***
 * Encapsulates the launch and transition logic for pipelines.
 * <p>
 * Note that the methods
 * {@link #launch(PipelineDefinition, String, PipelineDefinitionNode, PipelineDefinitionNode, String)}
 * and {@link #transitionToNextInstanceNode(PipelineInstance, PipelineTask, TaskCounts)} must not be
 * run in the context of a transaction; these methods provide their own transactions in order to
 * ensure that the transactions are completed before any task requests can be sent. Other methods,
 * including {@link #restartFailedTasks(Collection, boolean, RunMode)}, can (or in some cases must)
 * be run in a transaction context.
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
    private PipelineOperations pipelineOperations;

    public PipelineExecutor() {

        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
        pipelineInstanceCrud = new PipelineInstanceCrud();
        pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        pipelineTaskCrud = new PipelineTaskCrud();
        pipelineOperations = new PipelineOperations();
    }

    /**
     * Launch a new {@link PipelineInstance} for this {@link PipelineDefinition} with optional
     * startNode and/or endNode, plus an optional {@link String} for the name of a
     * {@link ParameterSet} created by a {@link ZiggyEventHandler}. This allows the event handler to
     * transfer information to the pipeline that it needs at runtime.
     */
    public PipelineInstance launch(PipelineDefinition pipeline, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode,
        String eventHandlerParamSetName) {

        List<PipelineInstanceNode> nodesForLaunch = new ArrayList<>();

        // Most of this method has to take place inside a database transaction.
        PipelineInstance pipelineInstance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                pipeline.buildPaths();

                /*
                 * Lock the current version of the model registry and associate it with this
                 * pipeline instance.
                 */
                ModelCrud modelCrud = new ModelCrud();
                ModelRegistry modelRegistry = modelCrud.lockCurrentRegistry();

                Priority priority = pipeline.getInstancePriority();

                PipelineInstance instance = new PipelineInstance();
                instance.setName(instanceName);
                instance.setPipelineDefinition(pipeline);
                instance.setState(PipelineInstance.State.PROCESSING);

                instance.startExecutionClock();
                instance.setPriority(priority);
                instance.setModelRegistry(modelRegistry);

                /*
                 * Set the pipeline instance params to the latest version of the name specified in
                 * the trigger and lock the param set. Also, if the pipeline instance needs a
                 * parameter set from a ZiggyEventHandler, add that to the parameters.
                 */
                Map<ClassWrapper<ParametersInterface>, String> triggerParamNames = pipeline
                    .getPipelineParameterSetNames();
                if (eventHandlerParamSetName != null) {
                    triggerParamNames.put(new ClassWrapper<>(ZiggyEventLabels.class),
                        eventHandlerParamSetName);
                }
                Map<ClassWrapper<ParametersInterface>, ParameterSet> instanceParams = instance
                    .getPipelineParameterSets();

                bindParameters(triggerParamNames, instanceParams);

                instance.setPipelineParameterSets(instanceParams);

                pipelineInstanceCrud.persist(instance);

                if (startNode == null) {
                    // start at the root
                    log.info(
                        "Creating instance nodes (starting at root because startNode not set)");

                    List<PipelineInstanceNode> rootInstanceNodes = new LinkedList<>();

                    for (PipelineDefinitionNode definitionRootNode : pipeline.getRootNodes()) {
                        PipelineInstanceNode rootInstanceNode = createInstanceNodes(instance,
                            pipeline, definitionRootNode, endNode);
                        rootInstanceNodes.add(rootInstanceNode);
                    }

                    nodesForLaunch.addAll(rootInstanceNodes);
                } else {
                    // start at the specified startNode
                    log.info(
                        "Creating instance nodes (startNode set, so starting there instead of root)");

                    PipelineInstanceNode startInstanceNode = createInstanceNodes(instance, pipeline,
                        startNode, endNode);
                    instance.setStartNode(startInstanceNode);

                    nodesForLaunch.add(startInstanceNode);
                }

                // make sure the new PipelineInstanceNodes are in the db for
                // launchNode, below
                DatabaseService.getInstance().flush();

                return instance;
            });

        for (PipelineInstanceNode node : nodesForLaunch) {
            launchNode(node);
        }

        // Get the current state of the pipeline instance from the database and return same.
        return (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> pipelineInstanceCrud.retrieve(pipelineInstance.getId()));
    }

    /**
     * The transition logic generates the worker task request messages for the next module in this
     * pipeline. This method only executes if the task in question completed successfully.
     */
    public void transitionToNextInstanceNode(PipelineInstanceNode instanceNode,
        TaskCounts currentNodeTaskCounts) {

        List<PipelineInstanceNode> nodesForLaunch = new ArrayList<>();
        List<PipelineDefinitionNode> nextPipelineDefinitionNodesNewUowTransition = new ArrayList<>();

        // Determine which pipeline definition nodes can perform a simple transition and which ones
        // require a new-UOW transition.
        DatabaseTransactionFactory.performTransaction(() -> {

            List<PipelineDefinitionNode> nextNodes = instanceNode.getPipelineDefinitionNode()
                .getNextNodes();
            nextPipelineDefinitionNodesNewUowTransition.addAll(nextNodes);
            return null;
        });

        // The new-UOW transitions need to do some additional database work.
        DatabaseTransactionFactory.performTransaction(() -> {

            // If there aren't any pipeline definition nodes to transition to, return.
            if (nextPipelineDefinitionNodesNewUowTransition.isEmpty()) {
                return null;
            }

            // Retrieve instance nodes for the remaining definition nodes (there might not
            // be any if we're at the end of the pipeline instance).
            for (PipelineDefinitionNode node : nextPipelineDefinitionNodesNewUowTransition) {
                PipelineInstanceNode nextInstanceNode = pipelineInstanceNodeCrud
                    .retrieve(instanceNode.getPipelineInstance(), node);
                if (nextInstanceNode != null) {
                    log.info("Launching node {} with a new UOW", node.getModuleName());
                    nodesForLaunch.add(nextInstanceNode);
                }
            }
            return null;
        });

        // Finally, launch the nodes.
        for (PipelineInstanceNode node : nodesForLaunch) {
            launchNode(node);
        }
    }

    /**
     * Logs the PipelineInstance state based on the aggregate PipelineInstanceNode task counts.
     * Called after the transition logic runs.
     */
    public void logUpdatedInstanceState(PipelineInstance pipelineInstance) {
        TaskCounts state = pipelineOperations.taskCounts(pipelineInstance);

        log.info("""
            updateInstanceState: all nodes:\s\
            numTasks/numSubmittedTasks/numCompletedTasks/numFailedTasks =\s\s\
            {} / {} / {} / {}""", state.getTaskCount(), state.getSubmittedTaskCount(),
            state.getCompletedTaskCount(), state.getFailedTaskCount());

        log.info("updateInstanceState: updated PipelineInstance.state = {} for id: {}",
            pipelineInstance.getState(), pipelineInstance.getId());
    }

    /**
     * Restart {@link PipelineTask}s in the ERROR state. This method performs the following steps:
     * <ol>
     * <li>Sets the {@link PipelineInstanceNode}s represented by the tasks into the not-yet-run
     * state.
     * <li>Checks each task to make sure it's in a condition that permits a resubmit.
     * <li>Sets the state of each task to SUBMITTED.
     * <li>Sends a task request for each task.
     * <li>Sets the instance states back to already-started state.
     * </ol>
     */
    public void restartFailedTasks(Collection<PipelineTask> tasks, boolean doTransitionOnly,
        RunMode restartMode) {
        log.debug("runTask(long) - start");

        // Map between the instance nodes and the tasks.
        Map<PipelineInstanceNode, List<PipelineTask>> nodesToTasks = new HashMap<>();
        for (PipelineTask task : tasks) {
            PipelineInstanceNode node = task.getPipelineInstanceNode();
            if (!nodesToTasks.containsKey(node)) {
                nodesToTasks.put(node, new ArrayList<>());
            }
            nodesToTasks.get(node).add(task);
        }

        // Perform restarts by instance node.
        for (Entry<PipelineInstanceNode, List<PipelineTask>> entry : nodesToTasks.entrySet()) {
            restartFailedTasksForNode(entry, doTransitionOnly, restartMode);
        }
    }

    /**
     * Performs task restart for all tasks associated with a particular
     * {@link PipelineInstanceNode}.
     */
    private void restartFailedTasksForNode(
        Map.Entry<PipelineInstanceNode, List<PipelineTask>> entry, boolean doTransitionOnly,
        RunMode restartMode) {
        PipelineInstanceNode node = entry.getKey();
        List<PipelineTask> tasks = entry.getValue();
        log.info("Restarting {} tasks for instance node {} ({})", tasks.size(), node.getId(),
            node.getPipelineDefinitionNode().getModuleName());

        pipelineOperations.taskCounts(node);
        logInstanceNodeCounts(node, "initial");

        // Loop over tasks and prepare for restart, including sending the task request message.
        for (PipelineTask task : tasks) {
            restartFailedTask(task, doTransitionOnly, restartMode);
        }

        // Update and log the instance state.
        PipelineInstance instance = node.getPipelineInstance();
        logUpdatedInstanceState(instance);
        pipelineInstanceCrud.merge(instance);

        logInstanceNodeCounts(node, "final");
    }

    private void logInstanceNodeCounts(PipelineInstanceNode node, String initialOrFinal) {
        TaskCounts instanceNodeCounts = pipelineOperations.taskCounts(node);
        log.info("""
            node {} state:\s\
            numTasks/numSubmittedTasks/numCompletedTasks/numFailedTasks =\s\s\
            {} / {} / {} / {}""", initialOrFinal, instanceNodeCounts.getTaskCount(),
            instanceNodeCounts.getSubmittedTaskCount(), instanceNodeCounts.getCompletedTaskCount(),
            instanceNodeCounts.getFailedTaskCount());
    }

    /**
     * Performs restart activities for a single {@link PipelineTask}.
     */
    private void restartFailedTask(PipelineTask task, boolean doTransitionOnly,
        RunMode restartMode) {
        boolean okayToRestart = false;
        PipelineTask.State oldState = task.getState();
        if (oldState == PipelineTask.State.ERROR) {
            okayToRestart = true;
        } else {
            ProcessingSummary processingState = new ProcessingSummaryOperations()
                .processingSummary(task.getId());
            if (processingState.getProcessingState() == ProcessingState.COMPLETE
                && processingState.getFailedSubtaskCount() > 0) {
                okayToRestart = true;
            }
        }

        log.info("Restarting failed task id=" + task.getId() + ", oldState : " + oldState);

        if (!okayToRestart) {
            log.warn("Task {} is in state {}, not restarting", task.getId(), oldState);
            return;
        }

        // Retrieve the task so that it can be modified in the database using the Hibernate
        // infrastructure
        PipelineTask databaseTask = pipelineTaskCrud.retrieve(task.getId());
        log.info("Restarting task id={}, oldState : {}", databaseTask.getId(), oldState);

        databaseTask.setRetry(true);
        pipelineOperations.setTaskState(databaseTask, PipelineTask.State.SUBMITTED);
        removeTaskFromKilledTaskList(pipelineTaskCrud.merge(databaseTask).getId());

        // Send the task message to the supervisor.
        sendTaskRequestMessage(databaseTask, Priority.HIGHEST, doTransitionOnly, restartMode);
    }

    /** Replace with dummy method in unit testing. */
    public void removeTaskFromKilledTaskList(long taskId) {
        ZiggyMessenger.publish(new RemoveTaskFromKilledTasksMessage(taskId));
    }

    /**
     * Generate and send out the tasks for the specified node.
     *
     * @param instanceNode
     * @param queueName
     * @return
     */
    private void launchNode(PipelineInstanceNode instanceNode) {
        Map<ClassWrapper<ParametersInterface>, ParameterSet> uowModuleParameterSets = instanceNode
            .getModuleParameterSets();
        log.debug("launchNode 1: start");

        launchNode(instanceNode, uowModuleParameterSets);
    }

    /**
     * Generate and send out the tasks for the specified node.
     *
     * @param instanceNode
     * @param queueName
     * @param uowModuleParameterSets
     * @return
     */
    private void launchNode(PipelineInstanceNode instanceNode,
        Map<ClassWrapper<ParametersInterface>, ParameterSet> uowModuleParameterSets) {
        PipelineInstance instance = instanceNode.getPipelineInstance();
        instanceNode.getPipelineDefinitionNode();
        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = unitOfWorkGenerator(
            instanceNode.getPipelineDefinitionNode());

        log.debug("launchNode 2: start");

        if (!unitOfWorkGenerator.isInitialized()) {
            throw new PipelineException(
                "Configuration Error: Unable to launch node because no UnitOfWorkGenerator class is defined");
        }

        Map<ClassWrapper<ParametersInterface>, ParameterSet> pipelineParameterSets = instance
            .getPipelineParameterSets();

        /*
         * Create a Map containing all of the entries from the pipeline parameters plus the module
         * parameters for this node for use by the UnitOfWorkTaskGenerator. This allows the UOW
         * parameters to be specified at either the pipeline or module level.
         */
        Map<ClassWrapper<ParametersInterface>, ParameterSet> compositeParameterSets = new HashMap<>(
            pipelineParameterSets);

        for (ClassWrapper<ParametersInterface> moduleParameterClass : uowModuleParameterSets
            .keySet()) {
            if (compositeParameterSets.containsKey(moduleParameterClass)) {
                throw new PipelineException(
                    "Configuration Error: Module parameter and pipeline parameter Maps both contain a value for parameter class: "
                        + moduleParameterClass);
            }
            compositeParameterSets.put(moduleParameterClass,
                uowModuleParameterSets.get(moduleParameterClass));
        }

        Map<Class<? extends ParametersInterface>, ParametersInterface> uowParams = new HashMap<>();

        for (ClassWrapper<ParametersInterface> parametersClass : compositeParameterSets.keySet()) {
            ParameterSet parameterSet = compositeParameterSets.get(parametersClass);
            Class<? extends ParametersInterface> clazz = parametersClass.getClazz();
            uowParams.put(clazz, parameterSet.parametersInstance());
        }

        log.debug("Generating tasks");
        List<UnitOfWork> tasks = new ArrayList<>();
        tasks = unitOfWorkGenerator.newInstance().generateUnitsOfWork(uowParams);
        log.info("Generated " + tasks.size() + " tasks for pipeline definition node "
            + instanceNode.getPipelineDefinitionNode().getModuleName());

        if (tasks.isEmpty()) {
            throw new PipelineException("Task generation did not generate any tasks!  UOW class: "
                + unitOfWorkGenerator.getClassName());
        }
        launchTasks(instanceNode, instance, tasks);
    }

    public ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineDefinitionNode node) {
        return UnitOfWorkGenerator.unitOfWorkGenerator(node);
    }

    /**
     * Recursive method to create a {@link PipelineInstanceNode} for all subsequent
     * {@link PipelineDefinitionNode}s This is later used by PipelineInstanceCrud.isNodeComplete()
     * to determine if all tasks are complete for a given node.
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
        pipelineInstanceNodeCrud.persist(instanceNode);

        Map<ClassWrapper<ParametersInterface>, String> pipelineNodeParameters = node
            .getModuleParameterSetNames();
        Map<ClassWrapper<ParametersInterface>, ParameterSet> instanceNodeParams = instanceNode
            .getModuleParameterSets();

        bindParameters(pipelineNodeParameters, instanceNodeParams);

        if (endNode != null && endNode.getId().equals(node.getId())) {
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
     * For each {@link ParamSetName}, retrieve the latest version of the {@link ParamSet}, lock it,
     * and put it into the params map. Used for both pipeline params and module params.
     *
     * @param paramNames
     * @param params
     */
    private void bindParameters(Map<ClassWrapper<ParametersInterface>, String> paramNames,
        Map<ClassWrapper<ParametersInterface>, ParameterSet> params) {
        for (ClassWrapper<ParametersInterface> paramClass : paramNames.keySet()) {
            String pipelineParamName = paramNames.get(paramClass);
            ParameterSet paramSet = parameterSetCrud
                .retrieveLatestVersionForName(pipelineParamName);
            params.put(paramClass, paramSet);
            paramSet.lock();
        }
    }

    /**
     * Create new {@link PipelineTask}s and the corresponding {@link TaskRequest} messages.
     */
    private void launchTasks(PipelineInstanceNode instanceNode, PipelineInstance instance,
        List<UnitOfWork> tasks) {
        List<PipelineTask> pipelineTasks = new ArrayList<>();
        for (UnitOfWork task : tasks) {
            PipelineTask pipelineTask = new PipelineTask(instance, instanceNode);

            pipelineTask.setState(PipelineTask.State.SUBMITTED);
            pipelineTask.setUowTaskParameters(task.getParameters());
            pipelineTasks.add(pipelineTask);
        }
        DatabaseTransactionFactory.performTransaction(new DatabaseTransaction<Void>() {
            @Override
            public Void transaction() {
                pipelineTaskCrud.persist(pipelineTasks);
                return null;
            }

            // Do not allow in the context of an existing transaction.
            @Override
            public boolean allowExistingTransaction() {
                return false;
            }
        });
        for (PipelineTask pipelineTask : pipelineTasks) {
            sendTaskRequestMessage(pipelineTask, pipelineTask.getPipelineInstance().getPriority(),
                false, RunMode.STANDARD);
        }
    }

    public void persistTaskResults(PipelineTask task) {
        sendTaskRequestMessage(task, Priority.HIGH, false, RunMode.STANDARD);
    }

    private void sendTaskRequestMessage(PipelineTask task, Priority priority,
        boolean doTransitionOnly, RunMode runMode) {

        if (!taskRequestEnabled()) {
            return;
        }

        log.debug("Generating worker task message for task=" + task.getId() + ", module="
            + task.getModuleName());

        TaskRequest taskRequest = new TaskRequest(task.pipelineInstanceId(),
            task.pipelineInstanceNodeId(), task.getPipelineDefinitionNode().getId(), task.getId(),
            priority, doTransitionOnly, runMode);

        ZiggyMessenger.publish(taskRequest);
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

    void setPipelineModuleDefinitionCrud(
        PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud) {
        this.pipelineModuleDefinitionCrud = pipelineModuleDefinitionCrud;
    }

    void setPipelineModuleParameterSetCrud(ParameterSetCrud parameterSetCrud) {
        this.parameterSetCrud = parameterSetCrud;
    }

    public void setPipelineOperations(PipelineOperations pipelineOperations) {
        this.pipelineOperations = pipelineOperations;
    }

    public boolean taskRequestEnabled() {
        return true;
    }
}
